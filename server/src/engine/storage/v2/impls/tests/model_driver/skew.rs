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
                lit::Lit,
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
    std::collections::HashMap,
};

fn initialize_or_reopen_model_driver(name: &str, mdl_uuid: Uuid) -> Model {
    let mut mdl_fields = IndexSTSeqCns::idx_init();
    mdl_fields.st_insert("username".into(), Field::new([Layer::str()].into(), false));
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
    factorial(l)
}

fn npr_compensated(l: usize) -> usize {
    const COMPENSATION_MULTIPLIER: usize = 2;
    // compensate for low quality randomness
    npr(l) * COMPENSATION_MULTIPLIER
}

#[test]
fn skewed_insert_update_delete() {
    let mut rng = test_utils::rng();
    let make_row: fn(RawStr) -> Row = |field_id_ptr: RawStr| {
        Row::new(
            PrimaryIndexKey::try_from_dc(Datacell::new_str("sayan".into()).into()).unwrap(),
            into_dict!(field_id_ptr => Datacell::new_str("pwd1".into())),
            DeltaVersion::genesis(),
            DeltaVersion::genesis(),
        )
    };
    decl! {
        let orig_actions: [fn(&Model, &Row, &Guard, RawStr)] = [
            // insert (t=0)
            |model, row, g, _| {
                const VERSION: DeltaVersion = DeltaVersion::__new(0);
                model.data().delta_state().append_new_data_delta_with(
                    DataDeltaKind::Insert,
                    row.clone(),
                    VERSION,
                    &g,
                );
            },
            // update (t=1)
            |model, row, g, _| {
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
            // update (t=2)
            |model, row, g, _| {
                const VERSION: DeltaVersion = DeltaVersion::__new(2);
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
            // delete (t=3)
            |model, row, g, _| {
                const VERSION: DeltaVersion = DeltaVersion::__new(3);
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
            // insert (t=4) with same key
            |model, row, g, row_field_ptr| {
                const VERSION: DeltaVersion = DeltaVersion::__new(4);
                let row_data = row.d_data().read();
                let new_row = Row::new(
                    PrimaryIndexKey::try_from_dc(Datacell::new_str("sayan".into())).unwrap(),
                    into_dict!{ row_field_ptr => Datacell::new_str("pwd2".into()) },
                    row_data.get_schema_version(),
                    VERSION,
                );
                model.data().delta_state().append_new_data_delta_with(
                    DataDeltaKind::Insert,
                    new_row,
                    VERSION,
                    &g,
                );
            },
            // upsert (t=5) with same key
            |model, row, g, row_field_ptr| {
                const VERSION: DeltaVersion = DeltaVersion::__new(5);
                let row_data = row.d_data().read();
                model.data().delta_state().append_new_data_delta_with(
                    DataDeltaKind::Upsert,
                    Row::new(
                        PrimaryIndexKey::try_from_dc(Datacell::new_str("sayan".to_owned().into_boxed_str())).unwrap(),
                        into_dict!{ row_field_ptr => Datacell::new_str("pwd3".to_owned().into_boxed_str()) },
                        row_data.get_schema_version(),
                        VERSION,
                    ),
                    VERSION,
                    &g,
                );
            }
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
                let field_id_ptr = unsafe { model.data_mut().model_mutator().allocate("password") };
                assert_eq!(field_id_ptr.as_bytes(), "password".as_bytes());
                let row = make_row(unsafe { field_id_ptr.clone() });
                // apply events
                for action in actions {
                    (action)(&model, &row, &g, unsafe { field_id_ptr.clone() });
                    assert_eq!(*row.d_key(), Lit::from("sayan"));
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
                // reopen + validate
                let mdl = initialize_or_reopen_model_driver(log_name, mdl_uuid);
                assert_eq!(mdl.data().primary_index().__raw_index().mt_len(), 1);
                let fields = mdl
                    .data()
                    .primary_index()
                    .__raw_index()
                    .mt_get(&Lit::new_str("sayan"), &g)
                    .unwrap()
                    .read()
                    .fields()
                    .iter()
                    .map(|(x, y)| (x.to_string(), y.clone()))
                    .collect::<HashMap<_, _>>();
                assert_eq!(fields, into_dict! { "password" => "pwd3" });
                // remove
                drop(mdl);
                FileSystem::remove_file(log_name).unwrap();
            }
        }
    })
}
