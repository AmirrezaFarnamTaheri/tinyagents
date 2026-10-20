//! Aggregating branch results and detecting cross-branch overlap.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::Serialize;

/// What the collector needs to know about one finished branch.
pub trait BranchReport {
    fn branch_id(&self) -> &str;
    fn succeeded(&self) -> bool;
    /// Files (relative to the branch root) the branch changed.
    fn changed_files(&self) -> &[String];
}

/// A file touched by more than one branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OverlapWarning {
    pub file: String,
    pub workers: Vec<String>,
}

/// Aggregate of a finished batch, results in input order.
#[derive(Debug, Clone)]
pub struct Collected<R> {
    pub results: Vec<R>,
    pub failures: usize,
    pub overlap_warnings: Vec<OverlapWarning>,
}

impl<R: BranchReport> Collected<R> {
    pub fn new(results: Vec<R>) -> Self {
        let failures = results.iter().filter(|r| !r.succeeded()).count();
        let overlap_warnings = overlap_warnings(&results);
        Self {
            results,
            failures,
            overlap_warnings,
        }
    }
}

impl<R> Collected<R> {
    pub fn total(&self) -> usize {
        self.results.len()
    }
    pub fn succeeded(&self) -> usize {
        self.results.len().saturating_sub(self.failures)
    }
}

/// Files changed by more than one branch.
pub fn overlap_warnings<R: BranchReport>(results: &[R]) -> Vec<OverlapWarning> {
    let per_worker: Vec<(String, Vec<PathBuf>)> = results
        .iter()
        .filter(|r| !r.changed_files().is_empty())
        .map(|r| {
            (
                r.branch_id().to_string(),
                r.changed_files().iter().map(PathBuf::from).collect(),
            )
        })
        .collect();
    let mut overlaps = BTreeMap::<PathBuf, BTreeSet<String>>::new();
    for (worker, files) in per_worker {
        for file in files {
            overlaps.entry(file).or_default().insert(worker.clone());
        }
    }
    overlaps
        .into_iter()
        .filter(|(_, workers)| workers.len() > 1)
        .map(|(file, workers)| OverlapWarning {
            file: file.to_string_lossy().into_owned(),
            workers: workers.into_iter().collect(),
        })
        .collect()
}
