use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct KeyValue {
    pub k: String,
    pub v: String,
}

#[derive(Debug, Clone)]
pub enum JobPhase {
    MapPhase,
    ReducePhase,
}

pub fn merge_name(job_name: &str, reduce_task: usize) -> String {
    format!("mrtmp.{}-res-{}", job_name, reduce_task)
}

pub fn reduce_name(job_name: &str, map_task: usize, reduce_task: usize) -> String {
    format!("mrtmp.{}-{}-{}", job_name, map_task, reduce_task)
}
