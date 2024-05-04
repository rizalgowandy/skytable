/*
 * Created on Mon Apr 22 2024
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

pub mod driver;
pub mod error;
mod util;
// workloads
pub mod workloads;

use {
    self::error::WorkloadResult,
    crate::{
        error::BenchResult,
        setup,
        stats::{self, ComprehensiveRuntimeStats},
        workload::driver::WorkloadDriver,
    },
    std::{fmt, future::Future, process, time::Instant},
    tokio::runtime::Builder,
};

pub fn run_bench<W: Workload>(w: W) -> BenchResult<ComprehensiveRuntimeStats> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(unsafe { setup::instance().threads() })
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let sig = tokio::signal::ctrl_c();
        let mut control_connection = w.setup_control_connection().await?;
        info!("initializing workload '{}'", W::ID);
        let wl_drv = WorkloadDriver::<W>::initialize().await?;
        info!("executing workload '{}'", W::ID);
        tokio::select! {
            r_ = wl_drv.run_workload() => {
                if let Err(e) = w.finish(&mut control_connection).await {
                    error!("failed to clean up database. {e}");
                }
                if r_.is_ok() {
                    info!("{}", w.workload_execution_summary());
                }
                r_.map_err(From::from)
            }
            _ = sig => {
                W::signal_stop();
                info!("received termination signal. cleaning up");
                if let Err(e) = w.finish(&mut control_connection).await {
                    error!("failed to clean up database. {e}");
                }
                process::exit(0x00);
            }
        }
    })
}

pub struct PayloadExecStats {
    netio_ttfb_micros: u128,
    netio_full_resp_micros: u128,
    exec_start: Instant,
    exec_stop: Instant,
}

impl PayloadExecStats {
    fn new(
        netio_ttfb_micros: u128,
        netio_full_resp_micros: u128,
        parse_start: Instant,
        parse_stop: Instant,
    ) -> Self {
        Self {
            netio_ttfb_micros,
            netio_full_resp_micros,
            exec_start: parse_start,
            exec_stop: parse_stop,
        }
    }
}

pub trait Workload {
    /// name of the workload
    const ID: &'static str;
    /// the control connection
    type ControlPort;
    /// workload context, forming a part of the full workload
    type WorkloadContext: Clone + Send + Sync + 'static;
    /// a workload task
    type WorkloadPayload: Clone + Send + Sync + 'static;
    /// the data connection
    type DataPort: Send + Sync;
    /// task execution context
    type TaskExecContext: Send + Sync;
    // main thread
    async fn setup_control_connection(&self) -> WorkloadResult<Self::ControlPort>;
    /// clean up
    async fn finish(&self, control: &mut Self::ControlPort) -> WorkloadResult<()>;
    /// return a summary of the workload executed
    fn workload_execution_summary(&self) -> impl fmt::Display {
        format!(
            "{} queries executed. benchmark complete.",
            stats::fmt_u64(self.total_queries() as u64)
        )
    }
    fn workload_description() -> Option<Box<str>> {
        None
    }
    // task
    fn total_queries(&self) -> usize;
    /// get the tasks for this workload
    fn generate_tasks() -> impl IntoIterator<Item = Self::WorkloadContext>;
    /// get the ID of this workload task
    fn task_id(t: &Self::WorkloadContext) -> &'static str;
    fn task_description(_: &Self::WorkloadContext) -> Option<Box<str>> {
        None
    }
    /// get the number of queries run for this task
    fn task_query_count(t: &Self::WorkloadContext) -> usize;
    /// set up this task
    fn task_setup(t: &Self::WorkloadContext);
    /// clean up this task's generated data
    fn task_cleanup(t: &Self::WorkloadContext);
    /// initialize the task execution context
    fn task_exec_context_init(t: &Self::WorkloadContext) -> Self::TaskExecContext;
    // worker methods
    /// setup up the worker connection
    fn setup_data_connection(
    ) -> impl Future<Output = WorkloadResult<Self::DataPort>> + Send + 'static;
    /// get the next payload
    fn fetch_next_payload() -> Option<Self::WorkloadPayload>;
    /// execute this payload. return a tuple indicating the full execution time
    fn execute_payload(
        ctx: &mut Self::TaskExecContext,
        data_port: &mut Self::DataPort,
        pl: Self::WorkloadPayload,
    ) -> impl Future<Output = WorkloadResult<PayloadExecStats>> + Send;
    /// signal to terminate all worker threads
    fn signal_stop();
}
