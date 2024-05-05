/*
 * Created on Sat Nov 18 2023
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

use crate::{
    args::{BenchConfig, BenchType, BenchWorkload},
    error, legacy,
    stats::{ComprehensiveRuntimeStats, RuntimeStats},
    workload::{self, workloads},
};

/*
    runner
*/

enum BenchStats {
    Standard(Vec<(&'static str, RuntimeStats)>),
    Advanced(ComprehensiveRuntimeStats),
}

pub fn run(bench: BenchConfig) -> error::BenchResult<()> {
    let stat = match bench.workload {
        BenchType::Workload(workload) => match workload {
            BenchWorkload::UniformV1 => {
                workload::run_bench(workloads::UniformV1Std::new()).map(BenchStats::Advanced)
            }
        },
        BenchType::Legacy(l) => {
            warn!("using `--engine` is now deprecated. please consider switching to `--workload`");
            legacy::run_bench(l).map(BenchStats::Standard)
        }
    }?;
    warn!("benchmarks might appear to be slower. this tool is currently experimental");
    // print results
    self::print_table(stat);
    Ok(())
}

fn print_table(stat: BenchStats) {
    match stat {
        BenchStats::Advanced(data) => data.display(),
        BenchStats::Standard(data) => {
            println!("+---------+--------------------------+-----------------------+------------------------+");
            println!("| Query   | Effective real-world QPS | Slowest Query (nanos) | Fastest Query (nanos)  |");
            println!("+---------+--------------------------+-----------------------+------------------------+");
            for (query, RuntimeStats { qps, head, tail }) in data {
                println!("| {query:<7} | {qps:>24.2} | {tail:>21} | {head:>22} |",);
            }
            println!("+---------+--------------------------+-----------------------+------------------------+");
        }
    }
}
