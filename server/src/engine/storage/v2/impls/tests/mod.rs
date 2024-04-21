/*
 * Created on Thu Feb 22 2024
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

use crate::engine::{
    fractal::test_utils::TestGlobal,
    ql::{
        ast::{self, traits::ASTNode},
        tests::lex_insecure,
    },
};

mod gns;
mod model_driver;

fn exec<'a, N: ASTNode<'a>, T>(
    global: &TestGlobal,
    query: &'a str,
    and_then: impl FnOnce(&TestGlobal, N) -> T,
) -> T {
    self::exec_step(global, query, 2, and_then)
}

fn exec_step<'a, N: ASTNode<'a>, T>(
    global: &TestGlobal,
    query: &'a str,
    step: usize,
    and_then: impl FnOnce(&TestGlobal, N) -> T,
) -> T {
    let tokens = lex_insecure(query.as_bytes()).unwrap();
    let query: N =
        ast::parse_ast_node_full(unsafe { core::mem::transmute(&tokens[step..]) }).unwrap();
    let r = and_then(global, query);
    r
}
