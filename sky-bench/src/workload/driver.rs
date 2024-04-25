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
        error::{WorkloadDriverError, WorkloadResult},
        Workload,
    },
    crate::{
        setup,
        stats::{self, WorkerLocalStats},
        workload::GeneratedWorkload,
    },
    skytable::Config,
    std::{
        marker::PhantomData,
        time::{Duration, Instant},
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::{broadcast, mpsc},
    },
};

const TIMEOUT_DURATION: Duration = Duration::from_secs(60);

pub type EncodedQueryList = Vec<Box<[u8]>>;

mod global {
    use {
        std::sync::atomic::{AtomicUsize, Ordering},
        tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    };

    #[derive(Debug, PartialEq)]
    struct Workload {
        expected_response_size: usize,
        packets: super::EncodedQueryList,
    }
    pub static POSITION: AtomicUsize = AtomicUsize::new(0);
    static WORKLOAD_LOCK: RwLock<bool> = RwLock::const_new(true);
    static mut WORKLOAD: Workload = Workload {
        expected_response_size: 0,
        packets: vec![],
    };
    pub fn gset_position(position: usize) {
        POSITION.store(position, Ordering::Release)
    }
    pub async fn glck_exclusive() -> RwLockWriteGuard<'static, bool> {
        WORKLOAD_LOCK.write().await
    }
    pub async fn glck_shared() -> RwLockReadGuard<'static, bool> {
        WORKLOAD_LOCK.read().await
    }
    pub unsafe fn gworkload_step() -> Option<&'static [u8]> {
        let mut current = POSITION.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return None;
            }
            match POSITION.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Acquire,
            ) {
                Ok(new) => return Some(&*WORKLOAD.packets.as_ptr().add(new - 1)),
                Err(_current) => current = _current,
            }
        }
    }
    pub unsafe fn report_crash() {
        POSITION.store(0, Ordering::SeqCst);
    }
    pub unsafe fn gworkload_resp_size() -> usize {
        WORKLOAD.expected_response_size
    }
    pub unsafe fn deallocate_workload() {
        WORKLOAD.packets.clear()
    }
    pub unsafe fn push_workload(workload: Vec<Box<[u8]>>, expected_resp_size: usize) {
        WORKLOAD.packets = workload;
        WORKLOAD.expected_response_size = expected_resp_size;
    }
}

#[derive(Debug)]
pub struct WorkloadDriver<W: Workload> {
    connection_count: usize,
    work_result_rx: mpsc::Receiver<WorkloadResult<WorkerLocalStats>>,
    work_tx: broadcast::Sender<WorkerTask>,
    _wl: PhantomData<W>,
}

impl<W: Workload> WorkloadDriver<W> {
    pub async fn initialize(w: &W, config: Config) -> WorkloadResult<Self> {
        let connection_count = unsafe { setup::instance() }.connections();
        let (online_tx, mut online_rx) = mpsc::channel::<WorkloadResult<()>>(connection_count);
        let (work_tx, _) = broadcast::channel::<WorkerTask>(connection_count);
        let (work_result_tx, work_result_rx) = mpsc::channel(connection_count);
        for id in 0..connection_count {
            let this_online_tx = online_tx.clone();
            let this_work_rx = work_tx.subscribe();
            let this_work_result_tx = work_result_tx.clone();
            let this_config = config.clone();
            let winit_packets = w.worker_init_packets();
            tokio::spawn(async move {
                worker_task(
                    id,
                    this_config,
                    this_online_tx,
                    this_work_rx,
                    this_work_result_tx,
                    winit_packets,
                )
                .await
            });
        }
        let mut initialized = 0;
        while initialized != connection_count {
            match online_rx.recv().await {
                Some(result) => result?,
                None => return Err(WorkloadDriverError::Driver("worker task crashed".into())),
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
    pub async fn run_workload(mut self, workload: &W) -> WorkloadResult<Vec<(&'static str, f64)>> {
        let mut results = vec![];
        // insert
        self.run_workload_task(
            "upsert",
            workload.generate_upsert(),
            &mut results,
            workload.get_query_count(),
        )
        .await?;
        self.run_workload_task(
            "insert",
            workload.generate_insert(),
            &mut results,
            workload.get_query_count(),
        )
        .await?;
        // update
        self.run_workload_task(
            "update",
            workload.generate_update(),
            &mut results,
            workload.get_query_count(),
        )
        .await?;
        // select
        self.run_workload_task(
            "select",
            workload.generate_select(),
            &mut results,
            workload.get_query_count(),
        )
        .await?;
        // delete
        self.run_workload_task(
            "delete",
            workload.generate_delete(),
            &mut results,
            workload.get_query_count(),
        )
        .await?;
        let _ = self.work_tx.send(WorkerTask::Terminate);
        Ok(results)
    }
    async fn run_workload_task(
        &mut self,
        task_name: &'static str,
        task: GeneratedWorkload<(EncodedQueryList, usize)>,
        results: &mut Vec<(&'static str, f64)>,
        count: usize,
    ) -> WorkloadResult<()> {
        if let GeneratedWorkload::Workload((encoded_packets, resp_size)) = task {
            info!("executing workload task '{task_name}' with {count} queries");
            // lock
            let workload_lock = global::glck_exclusive().await;
            unsafe {
                global::gset_position(count);
                global::push_workload(encoded_packets, resp_size);
            }
            if self.work_tx.send(WorkerTask::GetReady).is_err() {
                return Err(WorkloadDriverError::Driver(format!(
                    "a background worker crashed or exited due to an unknown reason"
                )));
            }
            // FIXME(@ohsayan): there is a lot of unnecessary time spent in threading etc., so this is very very imprecise
            drop(workload_lock);
            info!("workload task '{task_name}' execution started");
            let mut i = 0;
            let mut global_start = None;
            let mut global_stop = None;
            while i != self.connection_count {
                match self.work_result_rx.recv().await {
                    Some(Ok(r)) => {
                        match global_start.as_mut() {
                            Some(global) => {
                                if r.start < *global {
                                    *global = r.start;
                                }
                            }
                            None => global_start = Some(r.start),
                        }
                        let this_stop =
                            r.start + Duration::from_nanos(r.elapsed.try_into().unwrap());
                        match global_stop.as_mut() {
                            Some(global) => {
                                if this_stop > *global {
                                    *global = this_stop;
                                }
                            }
                            None => global_stop = Some(this_stop),
                        }
                    }
                    Some(Err(e)) => {
                        return Err(WorkloadDriverError::Driver(format!("a worker failed. {e}")))
                    }
                    None => {
                        return Err(WorkloadDriverError::Driver(format!(
                            "a background worker crashed or exited due to an unknown reason"
                        )))
                    }
                }
                i += 1;
            }
            let qps = stats::qps(
                count,
                global_stop
                    .unwrap()
                    .duration_since(global_start.unwrap())
                    .as_nanos(),
            );
            results.push((task_name, qps));
            unsafe { global::deallocate_workload() }
            info!("workload task '{task_name}' completed");
        } else {
            info!("workload task '{task_name}' skipped by workload");
        }
        Ok(())
    }
}

impl<W: Workload> Drop for WorkloadDriver<W> {
    fn drop(&mut self) {
        let _ = self.work_tx.send(WorkerTask::Terminate);
    }
}

/*
    worker
*/

macro_rules! timeout {
    ($e_tx:expr, $op:expr, $f:expr) => {
        match tokio::time::timeout(TIMEOUT_DURATION, $f).await {
            Ok(r) => r,
            Err(_) => {
                let _ = $e_tx
                    .send(Err(WorkloadDriverError::Driver(format!(
                        "{} timed out",
                        $op
                    ))))
                    .await;
                return;
            }
        }
    };
}

#[derive(Debug, Clone, Copy)]
enum WorkerTask {
    GetReady,
    Terminate,
}

async fn worker_task(
    worker_id: usize,
    config: Config,
    online_tx: mpsc::Sender<WorkloadResult<()>>,
    mut work_rx: broadcast::Receiver<WorkerTask>,
    result_rx: mpsc::Sender<WorkloadResult<WorkerLocalStats>>,
    (post_init_request, post_init_resp): (Vec<u8>, Vec<u8>),
) {
    let init = async {
        /*
            initialize the worker connection.
            we use a TCP connection instead of the client library's TCP connection since that allows us to precisely time
            when the server responds, which is otherwise not possible
        */
        // connect
        let mut con = TcpStream::connect((config.host(), config.port())).await?;
        // prepare handshake
        let hs = format!(
            "H\0\0\0\0\0{username_length}\n{password_length}\n{username}{password}",
            username_length = config.username().len(),
            password_length = config.password().len(),
            username = config.username(),
            password = config.password(),
        )
        .into_bytes();
        // send client handshake
        con.write_all(&hs).await?;
        // read server handshake
        let mut hs_resp = [0u8; 4];
        con.read_exact(&mut hs_resp).await?;
        Ok((con, hs_resp))
    };
    let (con, hs_resp) = match init.await {
        Ok(hs_resp) => hs_resp,
        Err(e) => {
            let _ = online_tx.send(Err(WorkloadDriverError::Io(e))).await;
            return;
        }
    };
    match hs_resp {
        [b'H', 0, 0, 0] => {}
        [b'H', 0, 1, e] => {
            let _ = online_tx
                .send(Err(WorkloadDriverError::Db(format!(
                    "connection rejected by server with hs error code: {}",
                    e
                ))))
                .await;
            return;
        }
        _ => {
            let _ = online_tx
                .send(Err(WorkloadDriverError::Db(format!(
                    "server responded with unknown handshake {hs_resp:?}"
                ))))
                .await;
            return;
        }
    }
    // now run post init packets
    let mut con = con;
    if let Err(e) = timeout!(
        result_rx,
        "sending worker init query",
        con.write_all(&post_init_request)
    ) {
        let _ = online_tx.send(Err(WorkloadDriverError::Io(e))).await;
        return;
    }
    let mut post_init_resp_actual = vec![0; post_init_resp.len()];
    if let Err(e) = timeout!(
        result_rx,
        "reading worker init response",
        con.read_exact(&mut post_init_resp_actual)
    ) {
        let _ = online_tx.send(Err(WorkloadDriverError::Io(e))).await;
        return;
    }
    if post_init_resp_actual != post_init_resp {
        let _ = online_tx.send(Err(WorkloadDriverError::Db(format!("expected post init responses do not match. received {post_init_resp_actual:?} from server")))).await;
        return;
    }
    // we are ready to go
    let _ = online_tx.send(Ok(())).await;
    // wait to act
    loop {
        match work_rx.recv().await.unwrap() {
            WorkerTask::GetReady => {}
            WorkerTask::Terminate => break,
        };
        let mut head = u128::MAX;
        let mut tail = 0u128;
        let mut elapsed = 0u128;
        let mut read_buffer = vec![
            0;
            unsafe {
                // UNSAFE(@ohsayan): getting a ready command indicates that the main task has already set the workload up
                global::gworkload_resp_size()
            }
        ];
        let _work_permit_that_is_hard_to_get = global::glck_shared().await;
        let start = Instant::now();
        while let Some(work) = unsafe {
            // UNSAFE(@ohsayan): since we received an execution request, this is safe to do
            global::gworkload_step()
        } {
            let start = Instant::now();
            if con.write_all(work).await.is_err() {
                unsafe {
                    // UNSAFE(@ohsayan): we hit an error, no matter how many workers call this in parallel, this is safe
                    global::report_crash();
                    let _ = result_rx
                        .send(Err(WorkloadDriverError::Db(format!(
                            "worker-{worker_id} failed to send query from the server"
                        ))))
                        .await;
                    return;
                }
            }
            if con.read_exact(&mut read_buffer).await.is_err() {
                unsafe {
                    // UNSAFE(@ohsayan): see above
                    global::report_crash();
                    let _ = result_rx
                        .send(Err(WorkloadDriverError::Db(format!(
                            "worker-{worker_id} failed to read response from the server"
                        ))))
                        .await;
                    return;
                }
            }
            let stop = Instant::now();
            let full_query_execution_time = stop.duration_since(start).as_nanos();
            if full_query_execution_time < head {
                head = full_query_execution_time;
            }
            if full_query_execution_time > tail {
                tail = full_query_execution_time
            }
            elapsed += full_query_execution_time;
            // FIXME(@ohsayan): validate the response here
        }
        // we're done here
        let _ = result_rx
            .send(Ok(WorkerLocalStats {
                start,
                head,
                tail,
                elapsed,
            }))
            .await;
    }
}
