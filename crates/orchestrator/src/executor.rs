//! Runs one dbt command as a subprocess (argv, no shell) and turns what happened
//! into an [`Execution`]: an outcome class, the exit code, a bounded log tail and
//! the parsed `run_results.json`.
//!
//! Every attempt gets its own `--target-path`, so concurrent or retried attempts
//! never read each other's artifacts and a stale `run_results.json` from an older
//! attempt cannot be mistaken for this one's.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orch_core::retry::AttemptOutcome;
use orch_core::run_results::{self, ParsedRunResults};
use orch_core::DbtCommand;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;
use tokio::sync::watch;

/// Why the worker wants the process stopped before it exits on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    Running,
    Cancel,
    LeaseLost,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub dbt_bin: PathBuf,
    pub project_dir: PathBuf,
    /// Parent of the per-attempt target directories.
    pub work_dir: PathBuf,
    /// Extra environment for dbt (e.g. `DBT_DUCKDB_PATH`).
    pub env: Vec<(String, String)>,
    pub log_tail_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct Execution {
    pub outcome: AttemptOutcome,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub log_tail: String,
    pub results: Option<ParsedRunResults>,
}

#[derive(Clone)]
struct Tail {
    buf: Arc<Mutex<(VecDeque<String>, usize)>>,
    cap: usize,
}

impl Tail {
    fn new(cap: usize) -> Self {
        Self {
            buf: Arc::new(Mutex::new((VecDeque::new(), 0))),
            cap,
        }
    }
    fn push(&self, line: String) {
        let mut g = self.buf.lock().expect("tail lock");
        g.1 += line.len() + 1;
        g.0.push_back(line);
        while g.1 > self.cap && g.0.len() > 1 {
            let dropped = g.0.pop_front().expect("non-empty");
            g.1 -= dropped.len() + 1;
        }
    }
    fn text(&self) -> String {
        let g = self.buf.lock().expect("tail lock");
        g.0.iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

async fn pump<R: AsyncRead + Unpin>(r: R, tail: Tail, stream: &'static str) {
    let mut lines = BufReader::new(r).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(target: "dbt", stream, "{line}");
        tail.push(line);
    }
}

/// SIGKILL the child's whole process group. Killing only the direct child can
/// leave grandchildren holding the stdout pipe open, which would block log
/// draining (and keep running) long after the attempt "ended".
fn kill_group(pgid: Option<u32>) {
    if let Some(pid) = pgid {
        // SAFETY: killpg only sends a signal; the group id is our child's pid
        // because it was spawned with process_group(0).
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

/// Wait for the log pumps to hit EOF, but not forever: if a straggler still
/// holds the pipes after the main process exited, kill the group and move on.
async fn drain(pumps: [tokio::task::JoinHandle<()>; 2], pgid: Option<u32>) {
    for p in pumps {
        let abort = p.abort_handle();
        if tokio::time::timeout(Duration::from_secs(2), p)
            .await
            .is_err()
        {
            kill_group(pgid);
            abort.abort();
        }
    }
}

fn classify(
    exit_code: Option<i32>,
    results: &Result<ParsedRunResults, String>,
) -> (AttemptOutcome, Option<String>) {
    match (exit_code, results) {
        (Some(0), Ok(_)) => (AttemptOutcome::Succeeded, None),
        // dbt's exit 1 means "ran, but a node errored or a test failed"
        (Some(1), Ok(r)) => {
            let failed: Vec<&str> = r
                .nodes
                .iter()
                .filter(|n| n.status.is_failure())
                .map(|n| n.unique_id.as_str())
                .collect();
            (
                AttemptOutcome::NodeFailure,
                Some(format!("failed nodes: {}", failed.join(", "))),
            )
        }
        (Some(code), Err(e)) => (
            AttemptOutcome::InfraError,
            Some(format!("dbt exited {code}; {e}")),
        ),
        (Some(code), Ok(_)) => (
            AttemptOutcome::InfraError,
            Some(format!("dbt exited {code}")),
        ),
        (None, _) => (
            AttemptOutcome::InfraError,
            Some("dbt was killed by a signal".into()),
        ),
    }
}

pub async fn execute(
    cfg: &ExecutorConfig,
    cmd: &DbtCommand,
    attempt_dir: &str,
    timeout: Duration,
    mut stop: watch::Receiver<Stop>,
) -> Execution {
    let target = cfg.work_dir.join(attempt_dir);
    let tail = Tail::new(cfg.log_tail_bytes);
    let fail = |outcome, error: String, tail: &Tail| Execution {
        outcome,
        exit_code: None,
        error: Some(error),
        log_tail: tail.text(),
        results: None,
    };
    let _ = tokio::fs::remove_dir_all(&target).await;
    if let Err(e) = tokio::fs::create_dir_all(&target).await {
        return fail(
            AttemptOutcome::InfraError,
            format!("creating {}: {e}", target.display()),
            &tail,
        );
    }
    let argv = cmd.argv(
        &cfg.project_dir.to_string_lossy(),
        &target.to_string_lossy(),
    );
    let mut child = match Command::new(&cfg.dbt_bin)
        .args(&argv)
        .envs(cfg.env.iter().map(|(k, v)| (k, v)))
        .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // own process group, so a kill reaches anything dbt (or a wrapper script) spawned
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return fail(
                AttemptOutcome::InfraError,
                format!("spawning {}: {e}", cfg.dbt_bin.display()),
                &tail,
            )
        }
    };
    // captured now: `child.id()` is None once the child has been reaped
    let pgid = child.id();
    let pumps = [
        tokio::spawn(pump(
            child.stdout.take().expect("piped"),
            tail.clone(),
            "stdout",
        )),
        tokio::spawn(pump(
            child.stderr.take().expect("piped"),
            tail.clone(),
            "stderr",
        )),
    ];

    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let mut stop_open = true;
    let interrupted = loop {
        tokio::select! {
            status = child.wait() => break Ok(status),
            _ = &mut deadline => break Err(AttemptOutcome::TimedOut),
            changed = stop.changed(), if stop_open => {
                match (changed, *stop.borrow()) {
                    (Ok(()), Stop::Cancel) => break Err(AttemptOutcome::Cancelled),
                    (Ok(()), Stop::LeaseLost) => break Err(AttemptOutcome::Lost),
                    (Ok(()), Stop::Running) => {}
                    // sender dropped: nobody can stop us any more; the timeout still applies
                    (Err(_), _) => stop_open = false,
                }
            }
        }
    };

    let status = match interrupted {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            return fail(
                AttemptOutcome::InfraError,
                format!("waiting for dbt: {e}"),
                &tail,
            )
        }
        Err(outcome) => {
            kill_group(pgid);
            let _ = child.kill().await;
            drain(pumps, pgid).await;
            let why = match outcome {
                AttemptOutcome::TimedOut => {
                    format!("timed out after {}s; dbt killed", timeout.as_secs())
                }
                AttemptOutcome::Cancelled => "run cancelled; dbt killed".into(),
                _ => "lease lost; dbt killed".into(),
            };
            return fail(outcome, why, &tail);
        }
    };
    drain(pumps, pgid).await;

    let rr_path = target.join("run_results.json");
    let results = match tokio::fs::read_to_string(&rr_path).await {
        Ok(s) => run_results::parse(&s).map_err(|e| format!("unparseable run_results.json: {e}")),
        Err(_) => Err("no run_results.json".to_string()),
    };
    let exit_code = status.code();
    let (outcome, error) = classify(exit_code, &results);
    Execution {
        outcome,
        exit_code,
        error,
        log_tail: tail.text(),
        results: results.ok(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    //! These use small shell scripts standing in for the dbt binary, so the process
    //! handling (timeouts, kills, exit-code classification, artifact parsing) is
    //! tested without Python. Real dbt runs are in the integration tests.
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const SUCCESS: &str = include_str!("../../orch-core/tests/fixtures/run_results_success.json");
    const TEST_FAILURE: &str =
        include_str!("../../orch-core/tests/fixtures/run_results_test_failure.json");

    struct Fake {
        _dir: tempfile::TempDir,
        cfg: ExecutorConfig,
    }

    /// The script receives dbt's argv; it finds `--target-path` and may write a
    /// run_results.json fixture there.
    fn fake_dbt(body: &str, fixture: Option<&str>) -> Fake {
        let dir = tempfile::tempdir().unwrap();
        if let Some(f) = fixture {
            std::fs::write(dir.path().join("fixture.json"), f).unwrap();
        }
        let script = dir.path().join("dbt");
        let src = format!(
            "#!/bin/sh\nFIX='{}'\nwhile [ $# -gt 0 ]; do [ \"$1\" = --target-path ] && T=\"$2\"; shift; done\n{body}\n",
            dir.path().join("fixture.json").display()
        );
        std::fs::write(&script, src).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cfg = ExecutorConfig {
            dbt_bin: script,
            project_dir: dir.path().to_path_buf(),
            work_dir: dir.path().join("work"),
            env: vec![("FAKE_ENV".into(), "ok".into())],
            log_tail_bytes: 200,
        };
        Fake { _dir: dir, cfg }
    }

    fn cmd() -> DbtCommand {
        DbtCommand::parse("dbt build --select stg_orders").unwrap()
    }

    async fn run(f: &Fake, timeout: Duration) -> Execution {
        let (_tx, rx) = watch::channel(Stop::Running);
        execute(&f.cfg, &cmd(), "a1", timeout, rx).await
    }

    #[tokio::test]
    async fn success_parses_run_results_and_captures_logs() {
        let f = fake_dbt(
            "echo \"building env=$FAKE_ENV\"; cp \"$FIX\" \"$T/run_results.json\"; exit 0",
            Some(SUCCESS),
        );
        let e = run(&f, Duration::from_secs(10)).await;
        assert_eq!(e.outcome, AttemptOutcome::Succeeded);
        assert_eq!(e.exit_code, Some(0));
        assert_eq!(e.results.unwrap().nodes.len(), 13);
        assert!(e.log_tail.contains("building env=ok"));
    }

    #[tokio::test]
    async fn exit_one_with_results_is_a_node_failure() {
        let f = fake_dbt(
            "cp \"$FIX\" \"$T/run_results.json\"; exit 1",
            Some(TEST_FAILURE),
        );
        let e = run(&f, Duration::from_secs(10)).await;
        assert_eq!(e.outcome, AttemptOutcome::NodeFailure);
        assert!(e.error.unwrap().contains("test.jaffle.always_fails"));
        assert_eq!(e.results.unwrap().summary().failed, 1);
    }

    #[tokio::test]
    async fn crash_without_results_is_an_infra_error() {
        let f = fake_dbt("echo boom >&2; exit 2", None);
        let e = run(&f, Duration::from_secs(10)).await;
        assert_eq!(e.outcome, AttemptOutcome::InfraError);
        assert_eq!(e.exit_code, Some(2));
        assert!(e.log_tail.contains("boom"));
        // exit 0 but no artifact is also not a success
        let f = fake_dbt("exit 0", None);
        assert_eq!(
            run(&f, Duration::from_secs(10)).await.outcome,
            AttemptOutcome::InfraError
        );
    }

    #[tokio::test]
    async fn timeout_kills_the_process() {
        let f = fake_dbt("sleep 30", None);
        let t0 = std::time::Instant::now();
        let e = run(&f, Duration::from_millis(300)).await;
        assert_eq!(e.outcome, AttemptOutcome::TimedOut);
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "did not wait for sleep 30"
        );
    }

    #[tokio::test]
    async fn timeout_kills_grandchildren_too() {
        // a wrapper that forks: killing only `sh` would leave `sleep` holding the pipe
        let f = fake_dbt("sleep 30 & sleep 30; wait", None);
        let t0 = std::time::Instant::now();
        let e = run(&f, Duration::from_millis(300)).await;
        assert_eq!(e.outcome, AttemptOutcome::TimedOut);
        assert!(
            t0.elapsed() < Duration::from_secs(3),
            "took {:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn background_straggler_does_not_block_a_finished_attempt() {
        let f = fake_dbt(
            "(sleep 30 &); cp \"$FIX\" \"$T/run_results.json\"; exit 0",
            Some(SUCCESS),
        );
        let t0 = std::time::Instant::now();
        let e = run(&f, Duration::from_secs(20)).await;
        assert_eq!(e.outcome, AttemptOutcome::Succeeded);
        assert!(
            t0.elapsed() < Duration::from_secs(6),
            "took {:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn cancel_and_lease_loss_kill_the_process() {
        for (signal, expected) in [
            (Stop::Cancel, AttemptOutcome::Cancelled),
            (Stop::LeaseLost, AttemptOutcome::Lost),
        ] {
            let f = fake_dbt("sleep 30", None);
            let (tx, rx) = watch::channel(Stop::Running);
            let cfg = f.cfg.clone();
            let h = tokio::spawn(async move {
                execute(&cfg, &cmd(), "a1", Duration::from_secs(60), rx).await
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(signal).unwrap();
            let e = tokio::time::timeout(Duration::from_secs(5), h)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(e.outcome, expected);
        }
    }

    #[tokio::test]
    async fn stale_artifacts_from_a_previous_attempt_are_removed() {
        let f = fake_dbt("exit 0", None);
        let stale = f.cfg.work_dir.join("a1");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("run_results.json"), SUCCESS).unwrap();
        assert_eq!(
            run(&f, Duration::from_secs(10)).await.outcome,
            AttemptOutcome::InfraError
        );
    }

    #[tokio::test]
    async fn missing_binary_is_an_infra_error() {
        let mut f = fake_dbt("exit 0", None);
        f.cfg.dbt_bin = "/nonexistent/dbt".into();
        let e = run(&f, Duration::from_secs(10)).await;
        assert_eq!(e.outcome, AttemptOutcome::InfraError);
        assert!(e.error.unwrap().contains("spawning"));
    }

    #[test]
    fn log_tail_is_bounded() {
        let t = Tail::new(20);
        for i in 0..100 {
            t.push(format!("line {i}"));
        }
        let s = t.text();
        assert!(s.len() <= 20 + 8, "{s}");
        assert!(s.ends_with("line 99"));
    }
}
