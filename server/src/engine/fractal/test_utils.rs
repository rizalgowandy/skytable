/*
 * Created on Wed Sep 13 2023
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
    super::{
        drivers::FractalGNSDriver, CriticalTask, FractalModelDriver, GenericTask, GlobalHealth,
        GlobalInstanceLike, Task,
    },
    crate::engine::{
        core::{EntityIDRef, GNSData, GlobalNS},
        data::uuid::Uuid,
        error::ErrorKind,
        storage::{
            safe_interfaces::{paths_v1, FileSystem, StdModelBatch},
            BatchStats, GNSDriver, ModelDriver,
        },
        RuntimeResult,
    },
    parking_lot::RwLock,
    std::sync::atomic::{AtomicUsize, Ordering},
};

/// A `test` mode global implementation
pub struct TestGlobal {
    gns: GlobalNS,
    lp_queue: RwLock<Vec<Task<GenericTask>>>,
    max_delta_size: usize,
    health: GlobalHealth,
    model_net_commited_events: AtomicUsize,
}

impl TestGlobal {
    fn new(gns: GlobalNS) -> Self {
        Self {
            gns,
            lp_queue: RwLock::default(),
            max_delta_size: usize::MAX,
            health: GlobalHealth::new(),
            model_net_commited_events: AtomicUsize::new(0),
        }
    }
    pub fn finish_into_driver(mut self) -> (GNSData, GNSDriver) {
        self.__close_all_model_drivers();
        let gns = core::mem::replace(
            &mut self.gns,
            GlobalNS::new(
                GNSData::empty(),
                FractalGNSDriver::new(GNSDriver::create_gns_with_name("xxxx").unwrap()),
            ),
        );
        drop(self);
        FileSystem::remove_file("xxxx").unwrap();
        let (data, drv) = gns.into_inner();
        (data, drv.txn_driver.into_inner())
    }
    pub fn set_max_data_pressure(&mut self, max_data_pressure: usize) {
        self.max_delta_size = max_data_pressure;
    }
    pub fn get_net_commited_events(&self) -> usize {
        self.model_net_commited_events.load(Ordering::Acquire)
    }
    /// Normally, model drivers are not loaded on startup because of shared global state. Calling this will attempt to load
    /// all model drivers
    fn load_model_drivers(&self) -> RuntimeResult<()> {
        let space_idx = self.gns.namespace().idx().read();
        for (model_name, model) in self.gns.namespace().idx_models().read().iter() {
            let model_data = model.data();
            let space_uuid = space_idx.get(model_name.space()).unwrap().get_uuid();
            let (driver, _) = ModelDriver::open_model_driver(
                model_data,
                &paths_v1::model_path(
                    model_name.space(),
                    space_uuid,
                    model_name.entity(),
                    model_data.get_uuid(),
                ),
                Default::default(),
            )?;
            model.driver().initialize_model_driver(driver);
        }
        Ok(())
    }
    fn __close_all_model_drivers(&mut self) {
        for (_, model) in self.gns.namespace().idx_models().write().iter_mut() {
            let delta_count = model
                .data()
                .delta_state()
                .__fractal_take_full_from_data_delta(super::FractalToken::new());
            self.model_net_commited_events
                .fetch_add(delta_count, Ordering::Release);
            if delta_count != 0 {
                let mut drv = model.driver().batch_driver().lock();
                drv.as_mut()
                    .unwrap()
                    .commit_with_ctx(
                        StdModelBatch::new(model.data(), delta_count),
                        BatchStats::new(),
                    )
                    .unwrap();
            }
            ModelDriver::close_driver(&mut model.driver().batch_driver().lock().as_mut().unwrap())
                .unwrap()
        }
    }
}

impl TestGlobal {
    pub fn new_with_driver_id_instant_update(log_name: &str) -> Self {
        let mut me = Self::new_with_driver_id(log_name);
        me.set_max_data_pressure(1);
        me
    }
    pub fn new_with_driver_id(log_name: &str) -> Self {
        let data = GNSData::empty();
        let driver = match GNSDriver::create_gns_with_name(log_name) {
            Ok(drv) => Ok(drv),
            Err(e) => match e.kind() {
                ErrorKind::IoError(e_) => match e_.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        GNSDriver::open_gns_with_name(log_name, &data, Default::default())
                            .map(|(jw, _)| jw)
                    }
                    _ => Err(e),
                },
                _ => Err(e),
            },
        }
        .unwrap();
        let me = Self::new(GlobalNS::new(data, FractalGNSDriver::new(driver)));
        me.load_model_drivers().unwrap();
        me
    }
}

impl GlobalInstanceLike for TestGlobal {
    fn health(&self) -> &GlobalHealth {
        &self.health
    }
    fn state(&self) -> &GlobalNS {
        &self.gns
    }
    fn taskmgr_post_high_priority(&self, task: Task<CriticalTask>) {
        match task.into_task() {
            CriticalTask::WriteBatch {
                model_id,
                observed,
                runtime_id,
            } => {
                let models = self.gns.namespace().idx_models().read();
                let mdl = models
                    .get(&EntityIDRef::new(model_id.space(), model_id.model()))
                    .unwrap();
                if runtime_id != mdl.data().runtime_id() {
                    return;
                }
                let mut mdl_driver = mdl.driver().batch_driver().lock();
                self.model_net_commited_events
                    .fetch_add(observed, Ordering::Release);
                mdl_driver
                    .as_mut()
                    .unwrap()
                    .commit_with_ctx(StdModelBatch::new(mdl.data(), observed), BatchStats::new())
                    .unwrap()
            }
            CriticalTask::TryModelAutorecover(_) => {}
            CriticalTask::CheckGNSDriver => {}
        }
    }
    fn taskmgr_post_standard_priority(&self, task: Task<GenericTask>) {
        self.lp_queue.write().push(task)
    }
    fn get_max_delta_size(&self) -> usize {
        self.max_delta_size
    }
    fn purge_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) {
        self.taskmgr_post_standard_priority(Task::new(GenericTask::delete_model_dir(
            space_name, space_uuid, model_name, model_uuid,
        )));
    }
    fn initialize_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> crate::engine::error::RuntimeResult<FractalModelDriver> {
        // create model dir
        FileSystem::create_dir_all(&paths_v1::model_dir(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        let driver = ModelDriver::create_model_driver(&paths_v1::model_path(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        Ok(super::drivers::FractalModelDriver::init(driver))
    }
}

impl Drop for TestGlobal {
    fn drop(&mut self) {
        {
            let mut txn_driver = self.gns.gns_driver().txn_driver.lock();
            GNSDriver::close_driver(&mut txn_driver).unwrap();
        }
        self.__close_all_model_drivers();
    }
}
