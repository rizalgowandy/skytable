/*
 * Created on Sun Jan 07 2024
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
    self::{
        impls::mdl_journal::{BatchStats, FullModel},
        raw::journal::{JournalSettings, RepairResult},
    },
    super::{common::interface::fs::FileSystem, v1, SELoaded},
    crate::{
        engine::{
            config::Configuration,
            core::{
                model::Model,
                system_db::{SystemDatabase, VerifyUser},
                EntityID, EntityIDRef, GNSData, GlobalNS,
            },
            fractal::{context, FractalGNSDriver},
            storage::{
                common::paths_v1,
                v2::raw::journal::{self, JournalRepairMode},
            },
            txn::{
                gns::{
                    model::CreateModelTxn,
                    space::CreateSpaceTxn,
                    sysctl::{AlterUserTxn, CreateUserTxn},
                },
                SpaceIDRef,
            },
            RuntimeResult,
        },
        util,
    },
    impls::{gns_log::GNSDriver, mdl_journal::ModelDriver},
};

pub(super) mod impls;
pub(super) mod raw;

pub const GNS_PATH: &str = v1::GNS_PATH;
pub const DATA_DIR: &str = v1::DATA_DIR;

pub fn recreate(gns: GNSData) -> RuntimeResult<SELoaded> {
    context::set_dmsg("creating gns");
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    reinit_full(&mut gns_driver, &gns, |model_id, model| {
        // re-initialize model
        let model_data = model.data();
        let space_uuid = gns.idx().read().get(model_id.space()).unwrap().get_uuid();
        FileSystem::create_dir_all(&paths_v1::model_dir(
            model_id.space(),
            space_uuid,
            model_id.entity(),
            model_data.get_uuid(),
        ))?;
        let mut model_driver = ModelDriver::create_model_driver(&paths_v1::model_path(
            model_id.space(),
            space_uuid,
            model_id.entity(),
            model_data.get_uuid(),
        ))?;
        model_driver.commit_with_ctx(FullModel::new(model_data), BatchStats::new())?;
        model.driver().initialize_model_driver(model_driver);
        Ok(())
    })?;
    Ok(SELoaded {
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}

fn reinit_full(
    gns_driver: &mut GNSDriver,
    gns: &GNSData,
    for_each_model: impl Fn(&EntityID, &Model) -> RuntimeResult<()>,
) -> RuntimeResult<()> {
    // create all spaces
    context::set_dmsg("creating all spaces");
    for (space_name, space) in gns.idx().read().iter() {
        FileSystem::create_dir_all(&paths_v1::space_dir(space_name, space.get_uuid()))?;
        gns_driver.commit_event(CreateSpaceTxn::new(space.props(), &space_name, space))?;
    }
    // create all users
    context::set_dmsg("creating all users");
    for (user_name, user) in gns.sys_db().users().read().iter() {
        gns_driver.commit_event(CreateUserTxn::new(&user_name, user.hash()))?;
    }
    // create all models
    context::set_dmsg("creating all models");
    for (model_id, model) in gns.idx_models().read().iter() {
        let model_data = model.data();
        let space_uuid = gns.idx().read().get(model_id.space()).unwrap().get_uuid();
        for_each_model(model_id, model)?;
        gns_driver.commit_event(CreateModelTxn::new(
            SpaceIDRef::with_uuid(model_id.space(), space_uuid),
            model_id.entity(),
            model_data,
        ))?;
    }
    Ok(())
}

pub fn initialize_new(config: &Configuration) -> RuntimeResult<SELoaded> {
    FileSystem::create_dir_all(DATA_DIR)?;
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    let gns = GNSData::empty();
    let password_hash = rcrypt::hash(&config.auth.root_key, rcrypt::DEFAULT_COST).unwrap();
    // now go ahead and initialize our root user
    gns_driver.commit_event(CreateUserTxn::new(
        SystemDatabase::ROOT_ACCOUNT,
        &password_hash,
    ))?;
    assert!(gns.sys_db().__raw_create_user(
        SystemDatabase::ROOT_ACCOUNT.to_owned().into_boxed_str(),
        password_hash.into_boxed_slice(),
    ));
    Ok(SELoaded {
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}

pub fn restore(cfg: &Configuration) -> RuntimeResult<SELoaded> {
    let gns = GNSData::empty();
    context::set_dmsg("loading gns");
    let (mut gns_driver, gns_driver_stats) =
        impls::gns_log::GNSDriver::open_gns(&gns, JournalSettings::default())?;
    if gns_driver_stats.compaction_recommended() {
        info!("compaction is recommeded for {GNS_PATH}");
    }
    let mut initialize_drivers = || {
        for (id, model) in gns.idx_models().write().iter_mut() {
            let model_data = model.data();
            let space_uuid = gns.idx().read().get(id.space()).unwrap().get_uuid();
            let model_data_file_path =
                paths_v1::model_path(id.space(), space_uuid, id.entity(), model_data.get_uuid());
            context::set_dmsg(format!("loading model driver in {model_data_file_path}"));
            let (model_driver, mdl_stats) = impls::mdl_journal::ModelDriver::open_model_driver(
                model_data,
                &model_data_file_path,
                JournalSettings::default(),
            )?;
            if mdl_stats.compaction_recommended() {
                info!("compaction is recommneded for {model_data_file_path}")
            }
            model.driver().initialize_model_driver(model_driver);
            unsafe {
                // UNSAFE(@ohsayan): all pieces of data are upgraded by now, so vacuum
                model.data_mut().model_mutator().vacuum_stashed();
            }
        }
        // check if password has changed
        if gns
            .sys_db()
            .verify_user(SystemDatabase::ROOT_ACCOUNT, cfg.auth.root_key.as_bytes())
            == VerifyUser::IncorrectPassword
        {
            // the password was changed
            warn!("root password changed via configuration");
            context::set_dmsg("updating password to system database from configuration");
            let phash = rcrypt::hash(&cfg.auth.root_key, rcrypt::DEFAULT_COST).unwrap();
            gns_driver.commit_event(AlterUserTxn::new(SystemDatabase::ROOT_ACCOUNT, &phash))?;
            gns.sys_db()
                .__raw_alter_user(SystemDatabase::ROOT_ACCOUNT, phash.into_boxed_slice());
        }
        RuntimeResult::Ok(())
    };
    match initialize_drivers() {
        Ok(()) => Ok(SELoaded {
            gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
        }),
        Err(e) => {
            error!("failed to load all storage drivers and/or data");
            info!("safely shutting down loaded drivers");
            for (id, model) in gns.idx_models().read().iter() {
                let mut batch_driver = model.driver().batch_driver().lock();
                let Some(mdl_driver) = batch_driver.as_mut() else {
                    continue;
                };
                if let Err(e) = ModelDriver::close_driver(mdl_driver) {
                    error!(
                        "failed to close model driver {}:{} due to error: {e}",
                        id.space(),
                        id.entity()
                    );
                }
            }
            if let Err(e) = GNSDriver::close_driver(&mut gns_driver) {
                error!("failed to close GNS driver due to error: {e}");
            }
            Err(e)
        }
    }
}

pub fn repair() -> RuntimeResult<()> {
    // back up all files
    let backup_dir = format!(
        "backups/{}-before-recovery-process",
        util::time_now_string()
    );
    context::set_dmsg("creating backup directory");
    FileSystem::create_dir_all(&backup_dir)?;
    context::set_dmsg("backing up GNS");
    FileSystem::copy(GNS_PATH, &format!("{backup_dir}/{GNS_PATH}"))?; // backup GNS
    context::set_dmsg("backing up data directory");
    FileSystem::copy_directory(DATA_DIR, &format!("{backup_dir}/{DATA_DIR}"))?; // backup data
    info!("All data backed up in {backup_dir}");
    // check and attempt repair: GNS
    let gns = GNSData::empty();
    context::set_dmsg("repair GNS");
    print_repair_info(
        journal::repair_journal::<raw::journal::EventLogAdapter<impls::gns_log::GNSEventLog>>(
            GNS_PATH,
            &gns,
            JournalSettings::default(),
            JournalRepairMode::Simple,
        )?,
        "GNS",
    );
    // check and attempt repair: models
    let models = gns.idx_models().read();
    for (space_id, space) in gns.idx().read().iter() {
        for model_id in space.models().iter() {
            let model = models.get(&EntityIDRef::new(&space_id, &model_id)).unwrap();
            let model_data_file_path = paths_v1::model_path(
                &space_id,
                space.get_uuid(),
                &model_id,
                model.data().get_uuid(),
            );
            context::set_dmsg(format!("repairing {model_data_file_path}"));
            print_repair_info(
                journal::repair_journal::<
                    raw::journal::BatchAdapter<impls::mdl_journal::ModelDataAdapter>,
                >(
                    &model_data_file_path,
                    model.data(),
                    JournalSettings::default(),
                    JournalRepairMode::Simple,
                )?,
                &model_data_file_path,
            )
        }
    }
    Ok(())
}

fn print_repair_info(result: RepairResult, id: &str) {
    match result {
        RepairResult::NoErrors => info!("repair: no errors detected in {id}"),
        RepairResult::UnspecifiedLoss(definitely_lost) => {
            if definitely_lost == 0 {
                warn!("repair: LOST DATA. repaired {id} but lost an unspecified amount of data")
            } else {
                warn!("repair: LOST DATA. repaired {id} but lost atleast {definitely_lost} trailing bytes")
            }
        }
    }
}
