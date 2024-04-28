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
        stats::{self, RuntimeStats, WorkerLocalStats},
    },
    std::{
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
    work_result_rx: mpsc::Receiver<WorkloadResult<WorkerLocalStats>>,
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
    pub async fn run_workload(mut self) -> WorkloadResult<Vec<(&'static str, RuntimeStats)>> {
        let mut results = vec![];
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
            let mut global_stop = None;
            let mut global_head = u128::MAX;
            let mut global_tail = 0;
            drop(permit_exclusive);
            // wait for all tasks to complete
            let mut workers_completed = 0;
            while workers_completed != self.connection_count {
                match self.work_result_rx.recv().await {
                    Some(Ok(WorkerLocalStats {
                        start,
                        elapsed,
                        head,
                        tail,
                    })) => {
                        let stop = start
                            .checked_add(Duration::from_nanos(elapsed.try_into().unwrap()))
                            .unwrap();
                        match global_start.as_mut() {
                            Some(gs) => {
                                if start < *gs {
                                    *gs = start;
                                }
                            }
                            None => global_start = Some(start),
                        }
                        match global_stop.as_mut() {
                            Some(gs) => {
                                if stop > *gs {
                                    *gs = stop;
                                }
                            }
                            None => global_stop = Some(stop),
                        }
                        if head < global_head {
                            global_head = head;
                        }
                        if tail > global_tail {
                            global_tail = tail;
                        }
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
                workers_completed += 1;
            }
            results.push((
                W::task_id(&task),
                RuntimeStats {
                    qps: stats::qps(
                        W::task_query_count(&task),
                        global_stop
                            .unwrap()
                            .duration_since(global_start.unwrap())
                            .as_nanos(),
                    ),
                    head: global_head,
                    tail: global_tail,
                },
            ));
            W::task_cleanup(&task);
        }
        Ok(results)
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
    result_rx: mpsc::Sender<WorkloadResult<WorkerLocalStats>>,
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
        let mut exec_ctx = W::task_exec_context_init(&workload_ctx);
        let work_permit = global::glck_shared().await;
        let local_start = Instant::now();
        let mut head = u128::MAX;
        let mut tail = 0;
        let mut net_elapsed = 0;
        while let Some(task) = W::fetch_next_payload() {
            let (start, stop) =
                match W::execute_payload(&mut exec_ctx, &mut worker_connection, task).await {
                    Ok(time) => time,
                    Err(e) => {
                        W::signal_stop();
                        let _ = result_rx.send(Err(e)).await;
                        return;
                    }
                };
            let this_elapsed = stop.duration_since(start).as_nanos();
            if this_elapsed > tail {
                tail = this_elapsed;
            }
            if this_elapsed < head {
                head = this_elapsed;
            }
            net_elapsed += this_elapsed;
        }
        drop(work_permit);
        let _ = result_rx
            .send(Ok(WorkerLocalStats::new(
                local_start,
                net_elapsed,
                head,
                tail,
            )))
            .await;
    }
}
