/*
 * Created on Tue Apr 23 2024
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
    skytable::error::Error,
    std::{fmt, io},
};

#[derive(Debug)]
pub enum WorkloadError {
    Io(io::Error),
    Db(String),
    Driver(String),
}

pub type WorkloadResult<T> = Result<T, WorkloadError>;

impl fmt::Display for WorkloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "i/o error: {e}"),
            Self::Db(e) => write!(f, "db error: {e}"),
            Self::Driver(e) => write!(f, "driver error: {e}"),
        }
    }
}

impl From<Error> for WorkloadError {
    fn from(e: Error) -> Self {
        Self::Db(format!(
            "direct operation on control connection failed: {e}"
        ))
    }
}

impl From<io::Error> for WorkloadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
