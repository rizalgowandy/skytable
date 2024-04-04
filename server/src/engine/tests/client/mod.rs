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

use skytable::{
    query,
    query::Pipeline,
    response::{Response, Value},
};

#[sky_macros::dbtest]
fn pipe() {
    let mut db = db!();
    let mut pipe = Pipeline::new();
    for _ in 0..100 {
        pipe.push(&query!("sysctl report status"));
    }
    assert_eq!(
        db.execute_pipeline(&pipe).unwrap(),
        vec![Response::Empty; 100]
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
