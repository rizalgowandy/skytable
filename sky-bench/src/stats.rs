/*
 * Created on Wed Apr 24 2024
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

use std::time::Instant;

pub fn qps(query_count: usize, time_taken_in_nanos: u128) -> f64 {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let time_taken_in_nanos_f64 = time_taken_in_nanos as f64;
    let query_count_f64 = query_count as f64;
    (query_count_f64 / time_taken_in_nanos_f64) * NANOS_PER_SECOND as f64
}

#[derive(Debug)]
pub struct RuntimeStats {
    pub qps: f64,
    pub head: u128,
    pub tail: u128,
}

#[derive(Debug)]
pub struct WorkerLocalStats {
    pub start: Instant,
    pub elapsed: u128,
    pub head: u128,
    pub tail: u128,
}

impl WorkerLocalStats {
    pub fn new(start: Instant, elapsed: u128, head: u128, tail: u128) -> Self {
        Self {
            start,
            elapsed,
            head,
            tail,
        }
    }
}
