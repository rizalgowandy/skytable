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

macro_rules! output_consts_group {
    (
        $(
            $constvis:vis const $constname:ident = $constexpr:expr;
        )*
        @yield $maxvis:vis const $maxident:ident;
        @yield $itemsvis:vis const $itemsident:ident;
    ) => {
        $(
            $constvis const $constname: &'static str = $constexpr;
        )*
        $maxvis const $maxident: usize = {
            let mut largest = 0;
            $(
                let l = str::len($constexpr);
                if l > largest {
                    largest = l;
                }
            )*
            largest
        };
        $itemsvis const $itemsident: [&'static str; {let mut i = 0; $(let _ = $constexpr; i += 1;)* i}] = [
            $($constname),*
        ];
    };
}
