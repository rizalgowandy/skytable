/*
 * Created on Sun Apr 28 2024
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

/*!
 * # `uniform_std_v1` workload
 *
 * This workload is a very real-world workload where we first create multiple unique rows using an `INSERT`, then mutate these rows using an `UPDATE`,
 * select a column using a `SELECT` and finally remove the row using `DELETE`.
 *
 * **We time a full execution**: Including encoding the entire query at execution time, sending the query, waiting to receive a response, validating and
 * parsing a response and allocating structures to store this response, like you would do in the real-world when using Skytable.
 *
 * This is very different from other benchmark tools, which often send the same query over multiple times.
*/

use {
    super::super::{
        error::{WorkloadError, WorkloadResult},
        util::{self, Target},
        Workload,
    },
    crate::{setup, workload::PayloadExecStats},
    skytable::{query, response::Response, ConnectionAsync, Pipeline, Query},
    std::{future::Future, sync::Arc, time::Instant},
};

static mut WL: WorkloadData = WorkloadData {
    queries: Vec::new(),
};

const DEFAULT_SPACE: &'static str = "db";
const DEFAULT_MODEL: &'static str = "db";

struct WorkloadData {
    queries: Vec<Query>,
}

#[derive(Debug)]
/// A real-worl, uniform distribution (INSERT,UPDATE,SELECT,DELETE in a 1:1:1:1 distribution) workload that inserts multiple unique rows, manipulates them,
/// selects them and finally removes them
pub struct UniformV1Std(());

impl UniformV1Std {
    pub fn new() -> Self {
        Self(())
    }
}

#[derive(Clone)]
pub struct UniformV1Task {
    id: &'static str,
    description: Arc<Box<str>>,
    f: fn(u64) -> Query,
}

impl UniformV1Task {
    fn new(id: &'static str, description: String, f: fn(u64) -> Query) -> Self {
        Self {
            id,
            description: Arc::new(description.into_boxed_str()),
            f,
        }
    }
}

impl Workload for UniformV1Std {
    const ID: &'static str = "uniform_std_v1";
    type ControlPort = ConnectionAsync;
    type WorkloadContext = UniformV1Task;
    type WorkloadPayload = &'static Query;
    type DataPort = ConnectionAsync;
    type TaskExecContext = ();
    fn workload_description() -> Option<Box<str>> {
        Some(
            format!("Unique rows created (INSERT), manipulated (UPDATE), fetched (SELECT) and deleted (DELETE) (1:1:1:1)").into_boxed_str()
        )
    }
    async fn setup_control_connection(&self) -> WorkloadResult<Self::ControlPort> {
        let mut con = util::setup_default_control_connection().await?;
        let ret = con
            .execute_pipeline(
                &Pipeline::new()
                    .add(&query!(format!("create space {DEFAULT_SPACE}")))
                    .add(&query!(format!(
                        "create model {DEFAULT_SPACE}.{DEFAULT_MODEL}(k: binary, v: uint64)"
                    ))),
            )
            .await?;
        if ret == vec![Response::Empty, Response::Empty] {
            Ok(con)
        } else {
            Err(WorkloadError::Db(format!(
                "failed to set up benchmarking space and model"
            )))
        }
    }
    async fn finish(&self, c: &mut Self::ControlPort) -> WorkloadResult<()> {
        c.query_parse::<()>(&query!(format!(
            "DROP SPACE ALLOW NOT EMPTY {DEFAULT_SPACE}"
        )))
        .await?;
        Ok(())
    }
    fn total_queries(&self) -> usize {
        unsafe { setup::instance() }.object_count() * 4
    }
    fn generate_tasks() -> impl IntoIterator<Item = Self::WorkloadContext> {
        let setup = unsafe { setup::instance() };
        [
            UniformV1Task::new(
                "INSERT",
                format!(
                    "Query='INS INTO {DEFAULT_MODEL}(?, ?)'; Params={}B binary key, 0 uint64 value",
                    setup.object_size()
                ),
                |unique_id| {
                    query!(
                        format!("INS into {DEFAULT_MODEL}(?, ?)"),
                        unsafe { setup::instance() }.fmt_pk(unique_id),
                        0u8
                    )
                },
            ),
            UniformV1Task::new(
                "UPDATE",
                format!(
                    "Query='UPD {DEFAULT_MODEL} SET v += ? WHERE k = ?'; Params={}B binary key, 1 uint64 value",
                    setup.object_size()
                ),
                |unique_id| {
                    query!(
                        format!("UPD {DEFAULT_MODEL} SET v += ? WHERE k = ?"),
                        1u8,
                        unsafe { setup::instance() }.fmt_pk(unique_id),
                    )
                },
            ),
            UniformV1Task::new(
                "SELECT",
                format!(
                    "Query='SEL v FROM {DEFAULT_MODEL} WHERE k = ?'; Params={}B binary key",
                    setup.object_size()
                ),
                |unique_id| {
                    query!(
                        format!("SEL v FROM {DEFAULT_MODEL} WHERE k = ?"),
                        unsafe { setup::instance() }.fmt_pk(unique_id),
                    )
                },
            ),
            UniformV1Task::new(
                "DELETE",
                format!(
                    "Query='DEL FROM {DEFAULT_MODEL} WHERE k = ?'; Params={}B binary key",
                    setup.object_size()
                ),
                |unique_id| {
                    query!(
                        format!("DEL FROM {DEFAULT_MODEL} WHERE k = ?"),
                        unsafe { setup::instance() }.fmt_pk(unique_id),
                    )
                },
            ),
        ]
    }
    fn task_id(t: &Self::WorkloadContext) -> &'static str {
        t.id
    }
    fn task_description(t: &Self::WorkloadContext) -> Option<Box<str>> {
        Some(t.description.as_ref().clone())
    }
    fn task_query_count(_: &Self::WorkloadContext) -> usize {
        unsafe { setup::instance() }.object_count()
    }
    fn task_setup(t: &Self::WorkloadContext) {
        let setup = &unsafe { setup::instance() };
        for i in 0..setup.object_count() {
            unsafe {
                WL.queries.push((t.f)(i as u64));
            }
        }
        Target::set(setup.object_count())
    }
    fn task_cleanup(_: &Self::WorkloadContext) {
        unsafe {
            WL.queries.clear();
        }
    }
    fn task_exec_context_init(_: &Self::WorkloadContext) -> Self::TaskExecContext {}
    async fn setup_data_connection() -> WorkloadResult<Self::DataPort> {
        let mut con = util::setup_default_control_connection().await?;
        con.query_parse::<()>(&query!(format!("use {DEFAULT_SPACE}")))
            .await?;
        Ok(con)
    }
    #[inline(always)]
    fn fetch_next_payload() -> Option<Self::WorkloadPayload> {
        Target::step(|new| unsafe { &*WL.queries.as_ptr().add(new - 1) })
    }
    #[inline(always)]
    fn execute_payload(
        _: &mut Self::TaskExecContext,
        data_port: &mut Self::DataPort,
        pl: Self::WorkloadPayload,
    ) -> impl Future<Output = WorkloadResult<PayloadExecStats>> + Send {
        async move {
            let start = Instant::now();
            // fully run a query by validating it, allocating any lists or maps or blobs
            // like you would in a real world application
            let (_, stat) = data_port.debug_query_latency(&pl).await?;
            let stop = Instant::now();
            Ok(PayloadExecStats::new(
                stat.ttfb_micros(),
                stat.full_resp(),
                start,
                stop,
            ))
        }
    }
    #[inline(always)]
    fn signal_stop() {
        Target::set_zero()
    }
}
