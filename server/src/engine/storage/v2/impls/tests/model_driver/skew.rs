/*
 * Created on Mon Apr 15 2024
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

/*
    skew tests are vital for ensuring integrity
*/

use {
    crate::{
        engine::{
            core::{
                index::{PrimaryIndexKey, Row},
                model::{
                    delta::{DataDeltaKind, DeltaVersion},
                    Field, Layer, Model, ModelData,
                },
            },
            data::{
                cell::Datacell,
                tag::{DataTag, FullTag},
                uuid::Uuid,
            },
            error::ErrorKind,
            fractal::FractalModelDriver,
            idx::{IndexBaseSpec, IndexSTSeqCns, MTIndex, STIndex},
            mem::RawStr,
            storage::{
                common::interface::fs::FileSystem,
                v2::{impls::mdl_journal::StdModelBatch, raw::journal::JournalSettings},
                BatchStats, ModelDriver,
            },
        },
        util::test_utils,
    },
    crossbeam_epoch::{pin, Guard},
    rand::seq::SliceRandom,
};

fn initialize_or_reopen_model_driver(name: &str, mdl_uuid: Uuid) -> Model {
    let mut mdl_fields = IndexSTSeqCns::idx_init();
    mdl_fields.st_insert("password".into(), Field::new([Layer::str()].into(), false));

    let mdl = Model::new(
        ModelData::new_restore(mdl_uuid, "username".into(), FullTag::STR, mdl_fields),
        FractalModelDriver::uninitialized(),
    );
    let mdl_driver = match ModelDriver::create_model_driver(name) {
        Ok(m) => m,
        Err(e) => match e.kind() {
            ErrorKind::IoError(io) => match io.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    ModelDriver::open_model_driver(mdl.data(), name, JournalSettings::default())
                        .unwrap()
                }
                _ => panic!("{e}"),
            },
            _ => panic!("{e}"),
        },
    };
    mdl.driver().initialize_model_driver(mdl_driver);
    mdl
}

fn factorial(mut l: usize) -> usize {
    let mut fctr = 1;
    while l != 0 {
        fctr *= l;
        l -= 1;
    }
    fctr
}

fn npr(l: usize) -> usize {
    factorial(l) / factorial(0)
}

fn npr_compensated(l: usize) -> usize {
    const COMPENSATION_MULTIPLIER: usize = 2;
    // compensate for low quality randomness
    npr(l) * COMPENSATION_MULTIPLIER
}

#[test]
fn skewed_insert_update_delete() {
    let mut rng = test_utils::rng();
    let make_row = |field_id_ptr: RawStr| {
        Row::new(
            PrimaryIndexKey::try_from_dc(Datacell::new_str("sayan".into()).into()).unwrap(),
            into_dict!(field_id_ptr => Datacell::new_str("pwd1".into())),
            DeltaVersion::genesis(),
            DeltaVersion::genesis(),
        )
    };
    decl! {
        let orig_actions: [fn(&Model, &Row, &Guard)] = [
            // insert (t=0)
            |model, row, g| {
                const VERSION: DeltaVersion = DeltaVersion::__new(0);
                model.data().delta_state().append_new_data_delta_with(
                    DataDeltaKind::Insert,
                    row.clone(),
                    VERSION,
                    &g,
                );
            },
            // update (t=1)
            |model, row, g| {
                const VERSION: DeltaVersion = DeltaVersion::__new(1);
                let mut row_data = row.d_data().write();
                if row_data.get_txn_revised() < VERSION {
                    row_data.set_txn_revised(VERSION);
                }
                model.data().delta_state().append_new_data_delta_with(
                    DataDeltaKind::Update,
                    row.clone(),
                    VERSION,
                    &g,
                );
            },
            // delete (t=2)
            |model, row, g| {
                const VERSION: DeltaVersion = DeltaVersion::__new(2);
                let mut row_data = row.d_data().write();
                if row_data.get_txn_revised() < VERSION {
                    row_data.set_txn_revised(VERSION);
                }
                model.data().delta_state().append_new_data_delta_with(
                    DataDeltaKind::Delete,
                    row.clone(),
                    VERSION,
                    &g,
                );
            },
        ];
    }
    test_utils::with_variable("skewed_insert_delete", |log_name| {
        /*
            iterate over all (hopefully) possible permutations of events
        */
        for _ in 0..npr_compensated(orig_actions.len()) {
            let mut actions = orig_actions;
            actions.shuffle(&mut rng);
            /*
                iterate over all possible batching sequences:
                [1]:[1,2], [1, 2]:[3], [1, 2, 3]:[]
            */
            for batching_sequence in 1..=orig_actions.len() {
                let batching_sequences =
                    [batching_sequence, orig_actions.len() - batching_sequence];
                /*
                    now init model
                */
                let g = pin();
                let mdl_uuid = Uuid::new();
                let mut model = initialize_or_reopen_model_driver(log_name, mdl_uuid);
                // create a row
                let row =
                    make_row(unsafe { model.data_mut().model_mutator().allocate("password") });
                // apply events
                for action in actions {
                    (action)(&model, &row, &g);
                }
                // commit and close
                {
                    {
                        let mut model_driver = model.driver().batch_driver().lock();
                        let model_driver = model_driver.as_mut().unwrap();
                        for observed_len in batching_sequences {
                            model_driver
                                .commit_with_ctx(
                                    StdModelBatch::new(model.data(), observed_len),
                                    BatchStats::new(),
                                )
                                .unwrap();
                        }
                        ModelDriver::close_driver(model_driver).unwrap();
                    }
                    drop(model);
                }
                // reopen
                let mdl = initialize_or_reopen_model_driver(log_name, mdl_uuid);
                assert_eq!(mdl.data().primary_index().__raw_index().mt_len(), 0);
                // remove
                drop(mdl);
                FileSystem::remove_file(log_name).unwrap();
            }
        }
    })
}
