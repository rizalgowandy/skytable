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

use {
    crate::{
        error::BenchResult,
        setup::{self, RunnerSetup},
        workload::driver::WorkloadDriver,
    },
    skytable::{query, Config, ConnectionAsync},
};

#[tokio::main]
pub async fn run_bench() -> BenchResult<()> {
    let setup = unsafe { setup::instance() };
    let config = Config::new(
        setup.host(),
        setup.port(),
        setup.username(),
        setup.password(),
    );
    let mut main_thread_db = config.connect_async().await?;
    let workload = UniformV1::new(setup);
    workload.initialize(&mut main_thread_db).await?;
    let ret = run(&workload, config).await;
    if let Err(e) = workload.cleanup(&mut main_thread_db).await {
        info!("failed to clean up DB: {e}");
    }
    ret
}

async fn run<W: Workload>(workload: &W, config: Config) -> BenchResult<()> {
    info!("initializing workload driver");
    let driver = WorkloadDriver::initialize(workload, config).await?;
    info!("beginning execution of workload {}", W::NAME);
    for (query, qps) in driver.run_workload(workload).await? {
        println!("{query}={qps:?}/sec");
    }
    Ok(())
}

/*
    uniform_v1
    -----
    - 1:1:1:1 Insert, select, update, delete
    - all unique rows
*/

pub struct UniformV1 {
    key_size: usize,
    query_count: usize,
}

impl UniformV1 {
    pub const DEFAULT_SPACE: &'static str = "db";
    pub const DEFAULT_MODEL: &'static str = "db";
    pub fn new(setup: &RunnerSetup) -> Self {
        Self {
            key_size: setup.object_size(),
            query_count: setup.object_count(),
        }
    }
    fn fmt_pk(&self, current: usize) -> Vec<u8> {
        format!("{current:0>width$}", width = self.key_size).into_bytes()
    }
}

impl Workload for UniformV1 {
    const NAME: &'static str = "uniform_v1";
    fn get_query_count(&self) -> usize {
        self.query_count
    }
    fn worker_init_packets(&self) -> (Vec<u8>, Vec<u8>) {
        (
            query!(format!("use {}", Self::DEFAULT_SPACE)).debug_encode_packet(),
            [0x12].into(),
        )
    }
    async fn initialize(&self, db: &mut ConnectionAsync) -> BenchResult<()> {
        db.query_parse::<()>(&query!(format!("create space {}", Self::DEFAULT_SPACE)))
            .await?;
        db.query_parse::<()>(&query!(format!(
            "create model {}.{}(k: binary, v: uint8)",
            Self::DEFAULT_SPACE,
            Self::DEFAULT_MODEL
        )))
        .await?;
        Ok(())
    }
    async fn cleanup(&self, db: &mut ConnectionAsync) -> BenchResult<()> {
        db.query_parse::<()>(&query!(format!(
            "drop model allow not empty {}.{}",
            Self::DEFAULT_SPACE,
            Self::DEFAULT_MODEL
        )))
        .await?;
        db.query_parse::<()>(&query!(format!("drop space {}", Self::DEFAULT_SPACE)))
            .await?;
        Ok(())
    }
    // workload generation
    fn generate_upsert(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)> {
        GeneratedWorkload::Skipped
    }
    fn generate_insert(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)> {
        let mut queries = vec![];
        for i in 0..self.query_count {
            queries.push(
                query!(
                    format!("ins into {}(?,?)", Self::DEFAULT_MODEL),
                    self.fmt_pk(i),
                    0u8
                )
                .debug_encode_packet()
                .into_boxed_slice(),
            );
        }
        // resp is the empty byte
        GeneratedWorkload::Workload((queries, 1))
    }
    fn generate_select(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)> {
        let mut queries = vec![];
        for i in 0..self.query_count {
            queries.push(
                query!(
                    format!("sel v from {} where k = ?", Self::DEFAULT_MODEL),
                    self.fmt_pk(i)
                )
                .debug_encode_packet()
                .into_boxed_slice(),
            );
        }
        // resp is {row_code}{row_size}\n{int_code}{int}\n
        GeneratedWorkload::Workload((queries, 6))
    }
    fn generate_update(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)> {
        let mut queries = vec![];
        for i in 0..self.query_count {
            queries.push(
                query!(
                    format!("upd {} set v += ? where k = ?", Self::DEFAULT_MODEL),
                    1u8,
                    self.fmt_pk(i)
                )
                .debug_encode_packet()
                .into_boxed_slice(),
            );
        }
        // resp is the empty byte
        GeneratedWorkload::Workload((queries, 1))
    }
    fn generate_delete(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)> {
        let mut queries = vec![];
        for i in 0..self.query_count {
            queries.push(
                query!(
                    format!("del from {} where k = ?", Self::DEFAULT_MODEL),
                    self.fmt_pk(i)
                )
                .debug_encode_packet()
                .into_boxed_slice(),
            );
        }
        // resp is the empty byte
        GeneratedWorkload::Workload((queries, 1))
    }
}

/*
    workload definition
*/

#[derive(Debug)]
pub enum GeneratedWorkload<T> {
    Workload(T),
    Skipped,
}

pub trait Workload: Sized {
    const NAME: &'static str;
    fn get_query_count(&self) -> usize;
    fn worker_init_packets(&self) -> (Vec<u8>, Vec<u8>);
    async fn initialize(&self, db: &mut ConnectionAsync) -> BenchResult<()>;
    fn generate_upsert(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)>;
    fn generate_insert(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)>;
    fn generate_select(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)>;
    fn generate_update(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)>;
    fn generate_delete(&self) -> GeneratedWorkload<(driver::EncodedQueryList, usize)>;
    async fn cleanup(&self, db: &mut ConnectionAsync) -> BenchResult<()>;
}
