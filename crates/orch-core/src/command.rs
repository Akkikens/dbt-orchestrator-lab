//! A job step is a dbt command line. It is parsed into argv (never run through a
//! shell) and restricted to the subcommands an orchestrator should run. Path flags
//! are owned by the worker, so a job cannot point dbt at another project or make
//! two attempts share a target directory.

use thiserror::Error;

const ALLOWED_SUBCOMMANDS: &[&str] = &["build", "run", "test", "seed", "snapshot"];
const WORKER_OWNED_FLAGS: &[&str] = &[
    "--project-dir",
    "--profiles-dir",
    "--target-path",
    "--log-path",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbtCommand {
    pub subcommand: String,
    pub args: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CommandError {
    #[error("command has unbalanced quotes")]
    Unparseable,
    #[error("command must start with `dbt`")]
    NotDbt,
    #[error("dbt subcommand `{0}` is not allowed (allowed: build, run, test, seed, snapshot)")]
    Subcommand(String),
    #[error("flag `{0}` is set by the worker and cannot appear in a job step")]
    ReservedFlag(String),
}

impl DbtCommand {
    pub fn parse(line: &str) -> Result<Self, CommandError> {
        let words = shlex::split(line).ok_or(CommandError::Unparseable)?;
        let mut it = words.into_iter();
        if it.next().as_deref() != Some("dbt") {
            return Err(CommandError::NotDbt);
        }
        let subcommand = it.next().unwrap_or_default();
        if !ALLOWED_SUBCOMMANDS.contains(&subcommand.as_str()) {
            return Err(CommandError::Subcommand(subcommand));
        }
        let args: Vec<String> = it.collect();
        for a in &args {
            let flag = a.split('=').next().unwrap_or(a);
            if WORKER_OWNED_FLAGS.contains(&flag) {
                return Err(CommandError::ReservedFlag(flag.to_string()));
            }
        }
        Ok(Self { subcommand, args })
    }

    /// argv after the `dbt` binary, with the worker-owned paths appended.
    pub fn argv(&self, project_dir: &str, target_path: &str) -> Vec<String> {
        let mut v = vec![self.subcommand.clone()];
        v.extend(self.args.iter().cloned());
        v.extend([
            "--project-dir".into(),
            project_dir.into(),
            "--profiles-dir".into(),
            project_dir.into(),
            "--target-path".into(),
            target_path.into(),
            "--log-path".into(),
            format!("{target_path}/logs"),
            "--no-use-colors".into(),
        ]);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_select_with_quotes() {
        let c = DbtCommand::parse(r#"dbt build --select "tag:nightly stg_orders+""#).unwrap();
        assert_eq!(c.subcommand, "build");
        assert_eq!(c.args, vec!["--select", "tag:nightly stg_orders+"]);
    }

    #[test]
    fn rejects_non_dbt_and_disallowed_subcommands() {
        assert_eq!(DbtCommand::parse("rm -rf /"), Err(CommandError::NotDbt));
        assert_eq!(
            DbtCommand::parse("dbt run-operation drop_all"),
            Err(CommandError::Subcommand("run-operation".into()))
        );
        assert_eq!(
            DbtCommand::parse("dbt build 'oops"),
            Err(CommandError::Unparseable)
        );
    }

    #[test]
    fn rejects_worker_owned_flags_in_both_forms() {
        assert_eq!(
            DbtCommand::parse("dbt build --target-path /tmp/x"),
            Err(CommandError::ReservedFlag("--target-path".into()))
        );
        assert_eq!(
            DbtCommand::parse("dbt run --project-dir=/etc"),
            Err(CommandError::ReservedFlag("--project-dir".into()))
        );
    }

    #[test]
    fn argv_appends_worker_paths() {
        let c = DbtCommand::parse("dbt test --select stg_orders").unwrap();
        let argv = c.argv("/p", "/t/a1");
        assert_eq!(&argv[..3], &["test", "--select", "stg_orders"]);
        assert!(argv.windows(2).any(|w| w == ["--target-path", "/t/a1"]));
        assert!(argv.windows(2).any(|w| w == ["--log-path", "/t/a1/logs"]));
    }
}
