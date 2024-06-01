/*
 * Created on Tue Apr 23 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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
    super::{
        error::{WorkloadError, WorkloadResult},
        Workload,
    },
    crate::{
        setup,
        stats::{
            self, ComprehensiveLatencyStats, ComprehensiveRuntimeStats, ComprehensiveWorkerStats,
            ComprehensiveWorkloadTaskStats, Histogram,
        },
    },
    std::{
        collections::LinkedList,
        marker::PhantomData,
        time::{Duration, Instant},
    },
    tokio::sync::{broadcast, mpsc},
};

mod global {
    use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
    static WORKLOAD_LOCK: RwLock<bool> = RwLock::const_new(true);
    pub async fn glck_exclusive() -> RwLockWriteGuard<'static, bool> {
        WORKLOAD_LOCK.write().await
    }
    pub async fn glck_shared() -> RwLockReadGuard<'static, bool> {
        WORKLOAD_LOCK.read().await
    }
}

#[derive(Debug)]
pub struct WorkloadDriver<W: Workload> {
    connection_count: usize,
    work_result_rx: mpsc::Receiver<WorkloadResult<ComprehensiveWorkerStats>>,
    work_tx: broadcast::Sender<WorkerCommand<W::WorkloadContext>>,
    _wl: PhantomData<W>,
}

impl<W: Workload> WorkloadDriver<W> {
    pub async fn initialize() -> WorkloadResult<Self> {
        let connection_count = unsafe { setup::instance() }.connections();
        let (online_tx, mut online_rx) = mpsc::channel::<WorkloadResult<()>>(connection_count);
        let (work_tx, _) =
            broadcast::channel::<WorkerCommand<W::WorkloadContext>>(connection_count);
        let (work_result_tx, work_result_rx) = mpsc::channel(connection_count);
        for _ in 0..connection_count {
            let this_online_tx = online_tx.clone();
            let this_work_rx = work_tx.subscribe();
            let this_work_result_tx = work_result_tx.clone();
            tokio::spawn(async move {
                worker_task::<W>(this_online_tx, this_work_rx, this_work_result_tx).await
            });
        }
        let mut initialized = 0;
        while initialized != connection_count {
            match online_rx.recv().await {
                Some(result) => result?,
                None => return Err(WorkloadError::Driver("worker task crashed".into())),
            }
            initialized += 1;
        }
        info!("all {initialized} workers online");
        Ok(Self {
            connection_count,
            work_result_rx,
            work_tx,
            _wl: PhantomData,
        })
    }
    pub async fn run_workload(mut self) -> WorkloadResult<ComprehensiveRuntimeStats> {
        let setup = unsafe { setup::instance() };
        let mut runtime_results = vec![];
        for task in W::generate_tasks() {
            let permit_exclusive = global::glck_exclusive().await;
            info!("running workload task '{}'", W::task_id(&task));
            // setup task
            W::task_setup(&task);
            // tell workers to get ready
            if self
                .work_tx
                .send(WorkerCommand::GetReady(task.clone()))
                .is_err()
            {
                W::signal_stop();
                return Err(WorkloadError::Driver(format!(
                    "a background worker crashed"
                )));
            }
            // prepare env and start
            let mut global_start = None;
            let mut global_stop_exec = None;
            let mut global_stop_netio = None;
            drop(permit_exclusive);
            // wait for all tasks to complete
            let mut worker_results = Vec::with_capacity(self.connection_count);
            while worker_results.len() != self.connection_count {
                match self.work_result_rx.recv().await {
                    Some(Ok(ComprehensiveWorkerStats {
                        thread_start,
                        server_latencies_micros,
                        full_latencies_micros,
                        netio_elapsed_micros,
                        exec_elapsed_nanos,
                    })) => {
                        let exec_stop = thread_start
                            .checked_add(Duration::from_nanos(
                                exec_elapsed_nanos.try_into().unwrap(),
                            ))
                            .unwrap();
                        let netio_stop = thread_start
                            .checked_add(Duration::from_micros(
                                netio_elapsed_micros.try_into().unwrap(),
                            ))
                            .unwrap();
                        match global_start.as_mut() {
                            Some(gs) => {
                                if thread_start < *gs {
                                    *gs = thread_start;
                                }
                            }
                            None => global_start = Some(thread_start),
                        }
                        match global_stop_exec.as_mut() {
                            Some(gs) => {
                                if exec_stop > *gs {
                                    *gs = exec_stop;
                                }
                            }
                            None => global_stop_exec = Some(exec_stop),
                        }
                        match global_stop_netio.as_mut() {
                            Some(gs) => {
                                if netio_stop > *gs {
                                    *gs = netio_stop;
                                }
                            }
                            None => global_stop_netio = Some(netio_stop),
                        }
                        worker_results.push((server_latencies_micros, full_latencies_micros));
                    }
                    Some(Err(e)) => {
                        W::signal_stop();
                        return Err(WorkloadError::Driver(format!("a worker failed. {e}")));
                    }
                    None => {
                        W::signal_stop();
                        return Err(WorkloadError::Driver(format!(
                            "a background worker failed due to an unknown reason"
                        )));
                    }
                }
            }
            // process latency report
            info!("workload task {} completed. now collating and computing latency results for this task", W::task_id(&task));
            let mut histogram_server_latencies = Histogram::initial();
            let mut histogram_full_latencies = Histogram::initial();
            worker_results.into_iter().for_each(
                |(server_latencies_micros, full_latencies_micros)| {
                    histogram_server_latencies.merge_latencies(server_latencies_micros);
                    histogram_full_latencies.merge_latencies(full_latencies_micros)
                },
            );
            // set global times
            let global_start = global_start.unwrap();
            let global_stop_exec = global_stop_exec.unwrap();
            let global_stop_netio = global_stop_netio.unwrap();
            // prepare stats
            let (server_latency_avg, server_latency_stdev) =
                histogram_server_latencies.get_avg_stdev();
            let (full_latency_avg, full_latency_stdev) = histogram_full_latencies.get_avg_stdev();
            runtime_results.push(ComprehensiveWorkloadTaskStats::new(
                W::task_id(&task).into(),
                W::task_description(&task),
                stats::qps_with_nanos(
                    W::task_query_count(&task),
                    global_stop_exec.duration_since(global_start).as_nanos(),
                ),
                stats::qps_with_nanos(
                    W::task_query_count(&task),
                    global_stop_netio.duration_since(global_start).as_nanos(),
                ),
                W::task_query_count(&task) as u64,
                ComprehensiveLatencyStats::new_with_microseconds(
                    server_latency_avg,
                    histogram_server_latencies.latency_min() as _,
                    histogram_server_latencies.latency_max() as _,
                    server_latency_stdev,
                ),
                ComprehensiveLatencyStats::new_with_microseconds(
                    full_latency_avg,
                    histogram_full_latencies.latency_min() as _,
                    histogram_full_latencies.latency_max() as _,
                    full_latency_stdev,
                ),
                histogram_server_latencies.prepare_distribution(),
                histogram_full_latencies.prepare_distribution(),
            ));
            W::task_cleanup(&task);
        }
        Ok(ComprehensiveRuntimeStats::new(
            format!("v{}", libsky::variables::VERSION).into_boxed_str(),
            format!("v{}", libsky::variables::VERSION).into_boxed_str(),
            "Skyhash/2.0".into(),
            W::ID.into(),
            format!(
                "threads={}, total clients={}; single-node (tcp@{}:{})",
                setup.threads(),
                setup.connections(),
                setup.host(),
                setup.port()
            )
            .into_boxed_str(),
            W::workload_description(),
            runtime_results,
        ))
    }
}

impl<W: Workload> Drop for WorkloadDriver<W> {
    fn drop(&mut self) {
        let _ = self.work_tx.send(WorkerCommand::Terminate);
    }
}

/*
    worker
*/

#[derive(Debug, Clone, Copy)]
enum WorkerCommand<W> {
    GetReady(W),
    Terminate,
}

async fn worker_task<W: Workload>(
    online_tx: mpsc::Sender<WorkloadResult<()>>,
    mut work_rx: broadcast::Receiver<WorkerCommand<W::WorkloadContext>>,
    result_rx: mpsc::Sender<WorkloadResult<ComprehensiveWorkerStats>>,
) {
    // initialize the worker connection
    let mut worker_connection = match W::setup_data_connection().await {
        Ok(con) => con,
        Err(e) => {
            let _ = online_tx.send(Err(e)).await;
            return;
        }
    };
    let _ = online_tx.send(Ok(())).await;
    loop {
        let workload_ctx = match work_rx.recv().await {
            Ok(WorkerCommand::GetReady(wctx)) => wctx,
            Ok(WorkerCommand::Terminate) | Err(_) => break,
        };
        let mut full_latencies = LinkedList::new();
        let mut server_latencies = LinkedList::new();
        let mut exec_ctx = W::task_exec_context_init(&workload_ctx);
        let work_permit = global::glck_shared().await;
        let local_start = Instant::now();
        let mut _elapsed_full_exec = 0;
        let mut _elapsed_netio_micros = 0;
        while let Some(task) = W::fetch_next_payload() {
            let payload_exec_stat =
                match W::execute_payload(&mut exec_ctx, &mut worker_connection, task).await {
                    Ok(time) => time,
                    Err(e) => {
                        W::signal_stop();
                        let _ = result_rx.send(Err(e)).await;
                        return;
                    }
                };
            server_latencies.push_back(payload_exec_stat.netio_ttfb_micros.try_into().unwrap());
            full_latencies.push_back(payload_exec_stat.netio_full_resp_micros.try_into().unwrap());
            _elapsed_full_exec += payload_exec_stat
                .exec_stop
                .duration_since(payload_exec_stat.exec_start)
                .as_nanos();
            _elapsed_netio_micros += payload_exec_stat.netio_full_resp_micros;
        }
        drop(work_permit);
        let _ = result_rx
            .send(Ok(ComprehensiveWorkerStats::new(
                local_start,
                _elapsed_full_exec,
                _elapsed_netio_micros,
                server_latencies,
                full_latencies,
            )))
            .await;
    }
}
