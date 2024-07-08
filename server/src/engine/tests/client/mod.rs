/*
 * Created on Mon Dec 04 2023
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
    skytable::{
        pipe, query,
        query::Pipeline,
        response::{Response, Rows, Value},
        Query, Response,
    },
    std::collections::HashMap,
};

const PIPE_RUNS: usize = 20;

#[sky_macros::dbtest]
fn pipe() {
    let mut db = db!();
    let mut pipe = Pipeline::new();
    for _ in 0..PIPE_RUNS {
        pipe.push(&query!("sysctl report status"));
    }
    assert_eq!(
        db.execute_pipeline(&pipe).unwrap(),
        vec![Response::Empty; PIPE_RUNS]
    );
}

#[sky_macros::dbtest]
fn pipe_params() {
    let mut db = db!();
    let pipe = Pipeline::new()
        .add(&query!("create space pipe_params"))
        .add(&query!(
            "create model pipe_params.pipe_model(username: string, pipes_per_day: uint64)"
        ))
        .add(&query!(
            "insert into pipe_params.pipe_model(?,?)",
            "sayan",
            0u64
        ))
        .add(&query!(
            "select * from pipe_params.pipe_model where username = ?",
            "sayan"
        ))
        .add(&query!("drop space allow not empty pipe_params"));
    let result = db.execute_pipeline(&pipe).unwrap();
    assert_eq!(
        &result[..3],
        vec![Response::Empty, Response::Empty, Response::Empty]
    );
    match &result[3] {
        Response::Row(r) => {
            assert_eq!(
                r.values(),
                [Value::String("sayan".into()), Value::UInt64(0)]
            )
        }
        unknown => panic!("expected row, got {unknown:?}"),
    }
    assert_eq!(result[4], Response::Empty);
}

#[sky_macros::dbtest]
fn truncate_test() {
    #[derive(Response, Query)]
    struct Entry {
        k: String,
        v: String,
    }
    impl Entry {
        fn new(k: &str, v: &str) -> Self {
            Self {
                k: k.to_string(),
                v: v.to_string(),
            }
        }
    }
    let mut db = db!();
    // init space and model, add data
    let pipe = pipe!(
        query!("create space truncation_tests"),
        query!("create model truncation_tests.entries(k: string, v: string)"),
        query!(
            "insert into truncation_tests.entries(?, ?)",
            Entry::new("world", "hello")
        ),
        query!(
            "insert into truncation_tests.entries(?, ?)",
            Entry::new("universe", "hello")
        )
    );
    assert!(db
        .execute_pipeline(&pipe)
        .unwrap()
        .into_iter()
        .all(|resp| resp == Response::Empty));
    // verify data
    let rows: Rows<Entry> = db
        .query_parse(&query!(
            "select all * from truncation_tests.entries limit ?",
            u64::MAX
        ))
        .unwrap();
    let rows: HashMap<_, _> = rows
        .into_rows()
        .into_iter()
        .map(|Entry { k, v }| (k, v))
        .collect();
    assert_eq!(rows.get("world").unwrap(), "hello");
    assert_eq!(rows.get("universe").unwrap(), "hello");
    // truncate
    db.query_parse::<()>(&query!("truncate model truncation_tests.entries"))
        .unwrap();
    // verify empty
    let rows: Rows<Entry> = db
        .query_parse(&query!(
            "select all * from truncation_tests.entries limit ?",
            u64::MAX
        ))
        .unwrap();
    assert_eq!(rows.len(), 0);
    // drop space & model
    db.query_parse::<()>(&query!("drop space allow not empty truncation_tests"))
        .unwrap();
}

#[sky_macros::dbtest(switch_user(username = "sneaky_guy"))]
fn truncate_is_root_only() {
    let mut db = db!();
    assert_eq!(
        db.query(&query!("truncate model truncation_tests.entries"))
            .unwrap(),
        Response::Error(5)
    );
}
