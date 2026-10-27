//! The I/O half of dbt-orchestrator-lab. Rules live in `orch_core`; this crate
//! applies them against Postgres, a dbt subprocess and Kafka.

pub mod api;
pub mod executor;
pub mod kafka;
pub mod projector;
pub mod relay;
pub mod scheduler;
pub mod store;
pub mod telemetry;
pub mod worker;

use anyhow::{Context, Result};
use orch_core::JobsFile;

/// Parse and validate a jobs TOML file (`[[jobs]]` tables).
pub fn load_jobs(path: &std::path::Path) -> Result<JobsFile> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let file: JobsFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    for j in &file.jobs {
        j.validate()?;
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    #[test]
    fn bundled_jobs_file_is_valid() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../jobs.toml");
        let jobs = super::load_jobs(&path).unwrap().jobs;
        assert!(jobs
            .iter()
            .any(|j| j.name == "jaffle_nightly" && j.cron.is_some()));
    }
}
