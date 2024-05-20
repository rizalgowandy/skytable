/*
 * This file is a part of Skytable
 *
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

/// The current version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The URL
pub const URL: &str = "https://github.com/skytable/skytable";

pub mod env_vars {
    /// the environment variable to set the password to use with any tool (skysh,sky-bench,..)
    pub const SKYDB_PASSWORD: &str = "SKYDB_PASSWORD";
}

pub mod test_utils {
    pub const DEFAULT_USER_NAME: &str = "root";
    pub const DEFAULT_USER_PASS: &str = "mypassword12345678";
    pub const DEFAULT_HOST: &str = "127.0.0.1";
    pub const DEFAULT_PORT: u16 = 2003;
}
