/*
 * Created on Sun Apr 28 2024
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
    super::error::WorkloadResult,
    crate::setup,
    skytable::{Config, ConnectionAsync},
    std::sync::atomic::{AtomicUsize, Ordering},
};

pub struct Target;
impl Target {
    #[inline(always)]
    fn _a() -> &'static AtomicUsize {
        static F: AtomicUsize = AtomicUsize::new(0);
        &F
    }
    #[inline(always)]
    pub fn set_zero() {
        Self::set(0)
    }
    #[inline(always)]
    pub fn set(v: usize) {
        Self::_a().store(v, Ordering::Release)
    }
    #[inline(always)]
    pub fn step<T>(f: impl Fn(usize) -> T) -> Option<T> {
        let mut current = Self::_a().load(Ordering::Acquire);
        loop {
            if current == 0 {
                return None;
            }
            match Self::_a().compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Acquire,
            ) {
                Ok(new) => return Some(f(new)),
                Err(_current) => current = _current,
            }
        }
    }
}

pub async fn setup_default_control_connection() -> WorkloadResult<ConnectionAsync> {
    let setup = unsafe { setup::instance() };
    let con = Config::new(
        setup.host(),
        setup.port(),
        setup.username(),
        setup.password(),
    )
    .connect_async()
    .await?;
    Ok(con)
}
