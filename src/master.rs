use crate::utils::*;
use crate::wc;

pub fn run<F: Fn(JobPhase)>(
    job_name: &str,
    files: Vec<String>,
    n_reduce: usize,
    schedule: F,
    finish: fn(),
) {
    println!("Start run: ");
    schedule(JobPhase::MapPhase);
    schedule(JobPhase::ReducePhase);
    finish();
    println!("Run finish");
}

#[allow(non_snake_case)]
pub fn sequential(
    job_name: &str,
    files: Vec<String>,
    n_reduce: usize,
    mapF: fn(&str, &str) -> Vec<KeyValue>,
    reduceF: fn(&str, &Vec<String>) -> String,
) {
    run(
        job_name,
        files,
        n_reduce,
        |phase| match phase {
            JobPhase::MapPhase => {
                mapF("1", "2");
            }
            JobPhase::ReducePhase => {
                reduceF("2", &vec!["3".to_owned()]);
            }
        },
        seq_finish,
    )
}

fn seq_finish() {}

fn wc_seq() {
    sequential("test", vec!["test".to_owned()], 1, wc::map, wc::reduce)
}
