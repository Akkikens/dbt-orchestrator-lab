//! Job definitions: a job is a DAG of dbt commands plus its scheduling policy.

use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::command::{CommandError, DbtCommand};
use crate::retry::RetryPolicy;
use crate::schedule::CronSchedule;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobsFile {
    pub jobs: Vec<JobSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobSpec {
    pub name: String,
    /// 5-field (minute resolution) or 6-field (with seconds) cron, UTC.
    #[serde(default)]
    pub cron: Option<String>,
    /// Maximum runs of this job in `running` at once. Extra runs wait in `queued`.
    #[serde(default = "one")]
    pub max_concurrency: u32,
    #[serde(default)]
    pub retry: RetryPolicy,
    /// Per-attempt wall-clock limit; the dbt process is killed when it is hit.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    pub steps: Vec<StepSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StepSpec {
    pub name: String,
    pub command: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

fn one() -> u32 {
    1
}
fn default_timeout() -> u64 {
    600
}

#[derive(Debug, Error, PartialEq)]
pub enum JobError {
    #[error("job name must be non-empty")]
    EmptyName,
    #[error("job `{0}` has no steps")]
    NoSteps(String),
    #[error("job `{job}`: {field} must be at least 1")]
    Zero { job: String, field: &'static str },
    #[error("job `{job}`: duplicate step `{step}`")]
    DuplicateStep { job: String, step: String },
    #[error("job `{job}`: step `{step}` depends on unknown step `{dep}`")]
    UnknownDependency {
        job: String,
        step: String,
        dep: String,
    },
    #[error("job `{job}`: steps form a cycle")]
    Cycle { job: String },
    #[error("job `{job}`, step `{step}`: {source}")]
    Command {
        job: String,
        step: String,
        source: CommandError,
    },
    #[error("job `{job}`: invalid cron `{expr}`: {reason}")]
    Cron {
        job: String,
        expr: String,
        reason: String,
    },
}

impl JobSpec {
    /// Reject anything the scheduler could not execute: bad commands, unknown or
    /// cyclic dependencies, zero limits, unparseable cron.
    pub fn validate(&self) -> Result<(), JobError> {
        let job = || self.name.clone();
        if self.name.trim().is_empty() {
            return Err(JobError::EmptyName);
        }
        if self.steps.is_empty() {
            return Err(JobError::NoSteps(job()));
        }
        for (field, v) in [
            ("max_concurrency", self.max_concurrency as u64),
            ("retry.max_attempts", self.retry.max_attempts as u64),
            ("timeout_secs", self.timeout_secs),
        ] {
            if v == 0 {
                return Err(JobError::Zero { job: job(), field });
            }
        }
        if let Some(expr) = &self.cron {
            CronSchedule::parse(expr).map_err(|reason| JobError::Cron {
                job: job(),
                expr: expr.clone(),
                reason,
            })?;
        }
        let mut names = HashSet::new();
        for s in &self.steps {
            if !names.insert(s.name.as_str()) {
                return Err(JobError::DuplicateStep {
                    job: job(),
                    step: s.name.clone(),
                });
            }
            DbtCommand::parse(&s.command).map_err(|source| JobError::Command {
                job: job(),
                step: s.name.clone(),
                source,
            })?;
        }
        for s in &self.steps {
            for d in &s.depends_on {
                if !names.contains(d.as_str()) {
                    return Err(JobError::UnknownDependency {
                        job: job(),
                        step: s.name.clone(),
                        dep: d.clone(),
                    });
                }
            }
        }
        if self.topological_order().is_none() {
            return Err(JobError::Cycle { job: job() });
        }
        Ok(())
    }

    /// Kahn's algorithm; `None` if the dependency graph has a cycle (including a
    /// step depending on itself). Ties keep declaration order so plans are stable.
    pub fn topological_order(&self) -> Option<Vec<&str>> {
        let mut indegree: HashMap<&str, usize> = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        for s in &self.steps {
            indegree.entry(&s.name).or_insert(0);
            for d in &s.depends_on {
                *indegree.entry(&s.name).or_insert(0) += 1;
                dependents.entry(d).or_default().push(&s.name);
            }
        }
        let mut ready: VecDeque<&str> = self
            .steps
            .iter()
            .map(|s| s.name.as_str())
            .filter(|n| indegree[n] == 0)
            .collect();
        let mut out = Vec::with_capacity(self.steps.len());
        while let Some(n) = ready.pop_front() {
            out.push(n);
            for m in dependents.get(n).into_iter().flatten() {
                let e = indegree.get_mut(m).expect("known step");
                *e -= 1;
                if *e == 0 {
                    ready.push_back(m);
                }
            }
        }
        (out.len() == self.steps.len()).then_some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(name: &str, deps: &[&str]) -> StepSpec {
        StepSpec {
            name: name.into(),
            command: "dbt build".into(),
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
        }
    }

    fn job(steps: Vec<StepSpec>) -> JobSpec {
        JobSpec {
            name: "j".into(),
            cron: None,
            max_concurrency: 1,
            retry: RetryPolicy::default(),
            timeout_secs: 60,
            steps,
        }
    }

    #[test]
    fn topological_order_respects_dependencies() {
        let j = job(vec![
            step("marts", &["staging", "seed"]),
            step("seed", &[]),
            step("staging", &["seed"]),
        ]);
        assert_eq!(
            j.topological_order().unwrap(),
            vec!["seed", "staging", "marts"]
        );
        j.validate().unwrap();
    }

    #[test]
    fn rejects_cycles_unknown_deps_and_duplicates() {
        let cyc = job(vec![step("a", &["b"]), step("b", &["a"])]);
        assert!(matches!(cyc.validate(), Err(JobError::Cycle { .. })));
        let selfdep = job(vec![step("a", &["a"])]);
        assert!(matches!(selfdep.validate(), Err(JobError::Cycle { .. })));
        let unknown = job(vec![step("a", &["ghost"])]);
        assert!(matches!(
            unknown.validate(),
            Err(JobError::UnknownDependency { .. })
        ));
        let dup = job(vec![step("a", &[]), step("a", &[])]);
        assert!(matches!(
            dup.validate(),
            Err(JobError::DuplicateStep { .. })
        ));
    }

    #[test]
    fn rejects_zero_limits_bad_cron_and_bad_commands() {
        let mut j = job(vec![step("a", &[])]);
        j.max_concurrency = 0;
        assert!(matches!(
            j.validate(),
            Err(JobError::Zero {
                field: "max_concurrency",
                ..
            })
        ));
        let mut j = job(vec![step("a", &[])]);
        j.cron = Some("not a cron".into());
        assert!(matches!(j.validate(), Err(JobError::Cron { .. })));
        let mut j = job(vec![step("a", &[])]);
        j.steps[0].command = "bash -c 'curl evil'".into();
        assert!(matches!(j.validate(), Err(JobError::Command { .. })));
    }
}
