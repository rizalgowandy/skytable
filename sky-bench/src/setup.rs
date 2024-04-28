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

use std::ptr::addr_of;

static mut SETUP: RunnerSetup = RunnerSetup {
    username: String::new(),
    password: String::new(),
    host: String::new(),
    port: 0,
    threads: 0,
    connections: 0,
    object_size: 0,
    object_count: 0,
};

#[derive(Debug)]
pub struct RunnerSetup {
    username: String,
    password: String,
    host: String,
    port: u16,
    threads: usize,
    connections: usize,
    object_size: usize,
    object_count: usize,
}

impl RunnerSetup {
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn password(&self) -> &str {
        &self.password
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn threads(&self) -> usize {
        self.threads
    }
    pub fn connections(&self) -> usize {
        self.connections
    }
    pub fn object_size(&self) -> usize {
        self.object_size
    }
    pub fn object_count(&self) -> usize {
        self.object_count
    }
    pub fn fmt_pk(&self, current: u64) -> Vec<u8> {
        Self::_fmt_pk(current, self.object_size())
    }
    fn _fmt_pk(current: u64, width: usize) -> Vec<u8> {
        format!("{current:0>width$}",).into_bytes()
    }
}

pub unsafe fn instance() -> &'static RunnerSetup {
    &*addr_of!(SETUP)
}

pub unsafe fn configure(
    username: String,
    password: String,
    host: String,
    port: u16,
    threads: usize,
    connections: usize,
    object_size: usize,
    object_count: usize,
) {
    SETUP.host = host;
    SETUP.port = port;
    SETUP.username = username;
    SETUP.password = password;
    SETUP.threads = threads;
    SETUP.connections = connections;
    SETUP.object_size = object_size;
    SETUP.object_count = object_count;
}

#[test]
fn fmt_pk() {
    assert_eq!(
        RunnerSetup::_fmt_pk(123456789, 18),
        "000000000123456789".as_bytes()
    );
}
