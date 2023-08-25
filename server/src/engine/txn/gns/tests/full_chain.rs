/*
 * Created on Fri Aug 25 2023
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

use crate::engine::{
    core::{
        space::{Space, SpaceMeta},
        GlobalNS,
    },
    data::{cell::Datacell, uuid::Uuid, DictEntryGeneric},
    ql::{ast::parse_ast_node_full, ddl::crt::CreateSpace, tests::lex_insecure},
    storage::v1::header_meta::HostRunMode,
    txn::gns::GNSTransactionDriverVFS,
};

fn double_run(f: impl FnOnce() + Copy) {
    f();
    f();
}

fn with_variable<T>(var: T, f: impl FnOnce(T)) {
    f(var);
}

fn init_txn_driver(gns: &GlobalNS, log_name: &str) -> GNSTransactionDriverVFS {
    GNSTransactionDriverVFS::open_or_reinit_with_name(&gns, log_name, 0, HostRunMode::Prod, 0)
        .unwrap()
}

fn init_space(
    gns: &GlobalNS,
    driver: &mut GNSTransactionDriverVFS,
    space_name: &str,
    env: &str,
) -> Uuid {
    let query = format!("create space {space_name} with {{ env: {env} }}");
    let stmt = lex_insecure(query.as_bytes()).unwrap();
    let stmt = parse_ast_node_full::<CreateSpace>(&stmt[2..]).unwrap();
    let name = stmt.space_name;
    Space::transactional_exec_create(&gns, driver, stmt).unwrap();
    gns.spaces().read().get(name.as_str()).unwrap().get_uuid()
}

#[test]
fn create_space() {
    with_variable("create_space_test.gns.db-tlog", |log_name| {
        let uuid;
        // start 1
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            uuid = init_space(&gns, &mut driver, "myspace", "{ SAYAN_MAX: 65536 }"); // good lord that doesn't sound like a good variable
            driver.close().unwrap();
        }
        double_run(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(
                gns.spaces().read().get("myspace").unwrap(),
                &Space::new_restore_empty(
                    SpaceMeta::with_env(
                        into_dict!("SAYAN_MAX" => DictEntryGeneric::Data(Datacell::new_uint(65536)))
                    ),
                    uuid
                )
            );
            driver.close().unwrap();
        })
    })
}

#[test]
fn alter_space() {
    with_variable("alter_space_test.gns.db-tlog", |log_name| {
        let uuid;
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            uuid = init_space(&gns, &mut driver, "myspace", "{}");
            let stmt =
                lex_insecure("alter space myspace with { env: { SAYAN_MAX: 65536 } }".as_bytes())
                    .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Space::transactional_exec_alter(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        double_run(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(
                gns.spaces().read().get("myspace").unwrap(),
                &Space::new_restore_empty(
                    SpaceMeta::with_env(
                        into_dict!("SAYAN_MAX" => DictEntryGeneric::Data(Datacell::new_uint(65536)))
                    ),
                    uuid
                )
            );
            driver.close().unwrap();
        })
    })
}

#[test]
fn drop_space() {
    with_variable("drop_space_test.gns.db-tlog", |log_name| {
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            let _ = init_space(&gns, &mut driver, "myspace", "{}");
            let stmt = lex_insecure("drop space myspace".as_bytes()).unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Space::transactional_exec_drop(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        double_run(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(gns.spaces().read().get("myspace"), None);
            driver.close().unwrap();
        })
    })
}
