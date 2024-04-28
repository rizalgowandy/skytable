/*
 * Created on Sat Nov 18 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use {
    crate::{
        args::{BenchConfig, BenchEngine, BenchType, BenchWorkload},
        error::{self, BenchResult},
        legacy::runtime::{fury, rookie},
        setup,
        stats::RuntimeStats,
        workload::{self, workloads},
    },
    skytable::{
        error::Error,
        query,
        response::{Response, Value},
        Config, Connection, Query,
    },
    std::{fmt, time::Instant},
};

pub const BENCHMARK_SPACE_ID: &'static str = "bench";
pub const BENCHMARK_MODEL_ID: &'static str = "bench";

/*
    task impl
*/

/// A bombard task used for benchmarking

#[derive(Debug)]
pub struct BombardTask {
    config: Config,
}

impl BombardTask {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

/// Errors while running a bombard
#[derive(Debug)]
pub enum BombardTaskError {
    DbError(Error),
    Mismatch,
}

impl fmt::Display for BombardTaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DbError(e) => write!(f, "a bombard subtask failed with {e}"),
            Self::Mismatch => write!(f, "got unexpected response for bombard subtask"),
        }
    }
}

impl From<Error> for BombardTaskError {
    fn from(dbe: Error) -> Self {
        Self::DbError(dbe)
    }
}

impl rookie::ThreadedBombardTask for BombardTask {
    type Worker = Connection;
    type WorkerTask = (Query, (BenchmarkTask, u64));
    type WorkerTaskSpec = BenchmarkTask;
    type WorkerInitError = Error;
    type WorkerTaskError = BombardTaskError;
    fn worker_init(&self) -> Result<Self::Worker, Self::WorkerInitError> {
        let mut db = self.config.connect()?;
        db.query_parse::<()>(&skytable::query!(format!("use {BENCHMARK_SPACE_ID}")))
            .map(|_| db)
    }
    fn generate_task(spec: &Self::WorkerTaskSpec, current: u64) -> Self::WorkerTask {
        (spec.generate_query(current), (*spec, current))
    }
    fn worker_drive_timed(
        worker: &mut Self::Worker,
        (query, (spec, current)): Self::WorkerTask,
    ) -> Result<u128, Self::WorkerTaskError> {
        let start = Instant::now();
        let ret = worker.query(&query)?;
        let stop = Instant::now();
        if spec.verify_response(current, ret) {
            Ok(stop.duration_since(start).as_nanos())
        } else {
            Err(BombardTaskError::Mismatch)
        }
    }
}

/*
    runner
*/

pub fn run(bench: BenchConfig) -> error::BenchResult<()> {
    let config_instance = unsafe { setup::instance() };
    let bench_config = BombardTask::new(Config::new(
        config_instance.host(),
        config_instance.port(),
        config_instance.username(),
        config_instance.password(),
    ));
    let mut main_thread_db = None;
    let stats = match bench.workload {
        BenchType::Workload(workload) => match workload {
            BenchWorkload::UniformV1 => workload::run_bench(workloads::UniformV1Std::new()),
        },
        BenchType::Legacy(l) => {
            warn!("using `--engine` is now deprecated. please consider switching to `--workload`");
            info!("running preliminary checks and creating model `bench.bench` with definition: `{{un: binary, pw: uint8}}`");
            let mut mt_db = bench_config.config.connect()?;
            mt_db.query_parse::<()>(&query!("create space bench"))?;
            mt_db.query_parse::<()>(&query!(format!(
                "create model {BENCHMARK_SPACE_ID}.{BENCHMARK_MODEL_ID}(un: binary, pw: uint8)"
            )))?;
            main_thread_db = Some(mt_db);
            match l {
                BenchEngine::Rookie => bench_rookie(bench_config),
                BenchEngine::Fury => bench_fury(),
            }
        }
    };
    let (total_queries, stats) = match stats {
        Ok(ret) => ret,
        Err(e) => {
            error!("benchmarking failed. attempting to clean up");
            match cleanup(main_thread_db) {
                Ok(()) => return Err(e),
                Err(e_cleanup) => {
                    error!("failed to clean up db: {e_cleanup}. please remove model `bench.bench` manually");
                    return Err(e);
                }
            }
        }
    };
    info!(
        "{} queries executed. benchmark complete.",
        fmt_u64(total_queries)
    );
    warn!("benchmarks might appear to be slower. this tool is currently experimental");
    // print results
    print_table(stats);
    cleanup(main_thread_db)?;
    Ok(())
}

/*
    util
*/

fn cleanup(main_thread_db: Option<Connection>) -> Result<(), error::BenchError> {
    trace!("dropping space and table");
    if let Some(mut db) = main_thread_db {
        db.query_parse::<()>(&query!("drop space allow not empty bench"))?;
    }
    Ok(())
}

fn print_table(data: Vec<(&'static str, RuntimeStats)>) {
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
    println!(
        "| Query   | Effective real-world QPS | Slowest Query (nanos) | Fastest Query (nanos)  |"
    );
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
    for (query, RuntimeStats { qps, head, tail }) in data {
        println!(
            "| {:<7} | {:>24.2} | {:>21} | {:>22} |",
            query, qps, tail, head
        );
    }
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
}

/*
    bench runner
*/

#[derive(Clone, Copy, Debug)]
pub struct BenchmarkTask {
    gen_query: fn(&Self, u64) -> Query,
    check_resp: fn(&Self, u64, Response) -> bool,
}

impl BenchmarkTask {
    fn new(
        gen_query: fn(&Self, u64) -> Query,
        check_resp: fn(&Self, u64, Response) -> bool,
    ) -> Self {
        Self {
            gen_query,
            check_resp,
        }
    }
    pub fn generate_query(&self, current: u64) -> Query {
        (self.gen_query)(self, current)
    }
    pub fn verify_response(&self, current: u64, resp: Response) -> bool {
        (self.check_resp)(self, current, resp)
    }
}

struct BenchItem {
    name: &'static str,
    spec: BenchmarkTask,
    count: usize,
}

impl BenchItem {
    fn new(name: &'static str, spec: BenchmarkTask, count: usize) -> Self {
        Self { name, spec, count }
    }
    fn print_log_start(&self) {
        info!(
            "benchmarking `{}`. average payload size = {} bytes. queries = {}",
            self.name,
            self.spec.generate_query(0).debug_encode_packet().len(),
            self.count
        )
    }
    fn run(self, pool: &mut rookie::BombardPool<BombardTask>) -> BenchResult<RuntimeStats> {
        pool.blocking_bombard(self.spec, self.count)
            .map_err(From::from)
    }
    async fn run_async(self, pool: &mut fury::Fury) -> BenchResult<RuntimeStats> {
        pool.bombard(self.count, self.spec)
            .await
            .map_err(From::from)
    }
}

fn prepare_bench_spec() -> Vec<BenchItem> {
    let config_instance = unsafe { setup::instance() };
    vec![
        BenchItem::new(
            "INSERT",
            BenchmarkTask::new(
                |_, current| {
                    query!(
                        "insert into bench(?, ?)",
                        unsafe { setup::instance() }.fmt_pk(current),
                        0u64
                    )
                },
                |_, _, actual_resp| actual_resp == Response::Empty,
            ),
            config_instance.object_count(),
        ),
        BenchItem::new(
            "SELECT",
            BenchmarkTask::new(
                |_, current| {
                    query!(
                        "select * from bench where un = ?",
                        unsafe { setup::instance() }.fmt_pk(current)
                    )
                },
                |_, current, resp| match resp {
                    Response::Row(r) => {
                        r.into_values()
                            == vec![
                                Value::Binary(unsafe { setup::instance() }.fmt_pk(current)),
                                Value::UInt8(0),
                            ]
                    }
                    _ => false,
                },
            ),
            config_instance.object_count(),
        ),
        BenchItem::new(
            "UPDATE",
            BenchmarkTask::new(
                |_, current| {
                    query!(
                        "update bench set pw += ? where un = ?",
                        1u64,
                        unsafe { setup::instance() }.fmt_pk(current)
                    )
                },
                |_, _, resp| resp == Response::Empty,
            ),
            config_instance.object_count(),
        ),
        BenchItem::new(
            "DELETE",
            BenchmarkTask::new(
                |_, current| {
                    query!(
                        "delete from bench where un = ?",
                        unsafe { setup::instance() }.fmt_pk(current)
                    )
                },
                |_, _, resp| resp == Response::Empty,
            ),
            config_instance.object_count(),
        ),
    ]
}

fn fmt_u64(n: u64) -> String {
    let num_str = n.to_string();
    let mut result = String::new();
    let chars_rev: Vec<_> = num_str.chars().rev().collect();
    for (i, ch) in chars_rev.iter().enumerate() {
        if i % 3 == 0 && i != 0 {
            result.push(',');
        }
        result.push(*ch);
    }
    result.chars().rev().collect()
}

fn bench_rookie(task: BombardTask) -> BenchResult<(u64, Vec<(&'static str, RuntimeStats)>)> {
    // initialize pool
    let config_instance = unsafe { setup::instance() };
    info!(
        "initializing connections. engine=rookie, threads={}, primary key size ={} bytes",
        config_instance.threads(),
        config_instance.object_size()
    );
    let mut pool = rookie::BombardPool::new(config_instance.threads(), task)?;
    // prepare benches
    let benches = prepare_bench_spec();
    // bench
    let total_queries = config_instance.object_count() as u64 * benches.len() as u64;
    let mut results = vec![];
    for task in benches {
        let name = task.name;
        task.print_log_start();
        let this_result = task.run(&mut pool)?;
        results.push((name, this_result));
    }
    Ok((total_queries, results))
}

fn bench_fury() -> BenchResult<(u64, Vec<(&'static str, RuntimeStats)>)> {
    let config_instance = unsafe { setup::instance() };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config_instance.threads())
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        info!(
            "initializing connections. engine=fury, threads={}, connections={}, primary key size ={} bytes",
            config_instance.threads(), config_instance.connections(), config_instance.object_size()
        );
        let mut pool = fury::Fury::new(
            config_instance.connections(),
            Config::new(config_instance.host(), config_instance.port(), config_instance.username(), config_instance.password()),
        )
        .await?;
        // prepare benches
        let benches = prepare_bench_spec();
        // bench
        let total_queries = config_instance.object_count() as u64 * benches.len() as u64;
        let mut results = vec![];
        for task in benches {
            let name = task.name;
            task.print_log_start();
            let this_result = task.run_async(&mut pool).await?;
            results.push((name, this_result));
        }
        Ok((total_queries,results))
    })
}
