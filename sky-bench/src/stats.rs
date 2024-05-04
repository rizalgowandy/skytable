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

use std::{collections::LinkedList, time::Instant};

pub fn qps_with_nanos(query_count: usize, time_taken_in_nanos: u128) -> f64 {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    fun_name(NANOS_PER_SECOND as _, time_taken_in_nanos, query_count)
}

fn fun_name(factor: f64, time: u128, count: usize) -> f64 {
    (count as f64 / time as f64) * factor
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

pub fn fmt_u64(n: u64) -> String {
    let num_str = n.to_string();
    let mut result = String::new();
    let chars_rev: Vec<_> = num_str.chars().rev().collect();
    for (i, ch) in chars_rev.iter().enumerate() {
        if i % 3 == 0 && i != 0 {
            result.push(',');
        }
        result.push(*ch);
    }
    result.chars().rev().collect()
}

fn fmt_f64_extra(number: f64, decimal_places: usize, suffix: &str) -> String {
    let number = number.to_string();
    let parts = number.split('.').collect::<Vec<&str>>();
    let int_part = parts[0].to_string();
    let dec_part = if parts.len() > 1 {
        parts[1][..decimal_places.min(parts[1].len())].to_string()
    } else {
        "0".repeat(decimal_places)
    };

    let mut formatted_int_part = String::new();
    let mut count = 0;

    for ch in int_part.chars().rev() {
        if count > 0 && count % 3 == 0 {
            formatted_int_part.insert(0, ',');
        }
        formatted_int_part.insert(0, ch);
        count += 1;
    }

    format!("{}.{}{suffix}", formatted_int_part, dec_part)
}

#[derive(Debug)]
pub struct ComprehensiveWorkerStats {
    pub thread_start: Instant,
    pub exec_elapsed_nanos: u128,
    pub netio_elapsed_micros: u128,
    pub server_latencies_micros: LinkedList<u64>,
    pub full_latencies_micros: LinkedList<u64>,
}

impl ComprehensiveWorkerStats {
    pub fn new(
        thread_start: Instant,
        net_elapsed_nanos: u128,
        netio_elapsed_nanos: u128,
        server_latencies: LinkedList<u64>,
        full_latencies: LinkedList<u64>,
    ) -> Self {
        Self {
            thread_start,
            netio_elapsed_micros: netio_elapsed_nanos,
            exec_elapsed_nanos: net_elapsed_nanos,
            server_latencies_micros: server_latencies,
            full_latencies_micros: full_latencies,
        }
    }
}

pub struct Histogram {
    latency_micros: Vec<u64>,
}

impl Histogram {
    const PERCENTILES: [f64; 11] = [99., 95., 90., 85., 80., 75., 70., 65., 60., 55., 50.];
    pub const fn initial() -> Self {
        Self {
            latency_micros: Vec::new(),
        }
    }
    pub fn merge_latencies(&mut self, latencies: impl IntoIterator<Item = u64>) {
        self.latency_micros.extend(latencies);
        self.latency_micros.sort_unstable();
    }
    pub fn get_avg_stdev(&self) -> (f64, f64) {
        // avg
        let mut sum = 0u128;
        for latency in self.latency_micros.iter().cloned() {
            sum += latency as u128;
        }
        let avg = sum as f64 / self.latency_micros.len() as f64;
        // stdev
        let mut variance = 0f64;
        for latency in self.latency_micros.iter().cloned() {
            variance += (latency as f64 - avg).powi(2);
        }
        let variance = variance / (self.latency_micros.len() - 1) as f64;
        let standard_deviation = variance.sqrt();
        (avg, standard_deviation)
    }
    pub fn latency_px(&self, x: f64) -> u64 {
        let idx = ((x / 100.0) * (self.latency_micros.len() - 1) as f64).floor() as usize;
        self.latency_micros[idx]
    }
    pub fn latency_min(&self) -> u64 {
        self.latency_micros[0]
    }
    pub fn latency_max(&self) -> u64 {
        self.latency_micros[self.latency_micros.len() - 1]
    }
    /// Returns the distribution of latencies as a vector of tuples (percentile, latency in milliseconds)
    pub fn prepare_distribution(&self) -> Vec<(f64, f64)> {
        Self::PERCENTILES
            .into_iter()
            .map(|percentile| (percentile, self.latency_px(percentile) as f64 / 1000.0))
            .collect()
    }
}

#[derive(Debug)]
pub struct ComprehensiveWorkloadTaskStats {
    workload_name: Box<str>,
    notes: Option<Box<str>>,
    throughput_qps_full: f64,
    throughput_qps_raw: f64,
    executed: u64,
    server_latency: ComprehensiveLatencyStats,
    full_latency: ComprehensiveLatencyStats,
    server_latency_distribution: Vec<(f64, f64)>,
    full_latency_distribution: Vec<(f64, f64)>,
}

impl ComprehensiveWorkloadTaskStats {
    pub fn new(
        workload_name: Box<str>,
        notes: Option<Box<str>>,
        throughput_qps_full: f64,
        throughput_qps_raw: f64,
        executed: u64,
        server_latency: ComprehensiveLatencyStats,
        full_latency: ComprehensiveLatencyStats,
        server_latency_distribution: Vec<(f64, f64)>,
        full_latency_distribution: Vec<(f64, f64)>,
    ) -> Self {
        Self {
            workload_name,
            notes,
            throughput_qps_full,
            throughput_qps_raw,
            executed,
            server_latency,
            full_latency,
            server_latency_distribution,
            full_latency_distribution,
        }
    }
}

#[derive(Debug)]
pub struct ComprehensiveLatencyStats {
    mean: f64,
    min: f64,
    max: f64,
    stdev: f64,
}

impl ComprehensiveLatencyStats {
    pub fn new_with_microseconds(mean: f64, min: f64, max: f64, stdev: f64) -> Self {
        Self {
            mean: mean / 1000.,
            min: min / 1000.,
            max: max / 1000.,
            stdev: stdev / 1000.,
        }
    }
}

#[derive(Debug)]
pub struct ComprehensiveRuntimeStats {
    database_config: Box<str>,
    benchmark_tool_config: Box<str>,
    protocol: Box<str>,
    workload: Box<str>,
    mode: Box<str>,
    workload_notes: Option<Box<str>>,
    task_results: Vec<ComprehensiveWorkloadTaskStats>,
}

output_consts_group! {
    const SUMMARY_HEADER_1_SKYD_VERSION = "Skytable (skyd)";
    const SUMMARY_HEADER_2_BENCH_VERSION = "sky-bench";
    const SUMMARY_HEADER_3_PROTOCOL_VERSION = "Protocol";
    const SUMMARY_HEADER_4_WORKLOAD = "Workload";
    const SUMMARY_HEADER_5_TOTAL_QUERIES = "Total queries";
    const SUMMARY_HEADER_6_MODE = "Mode";
    const SUMMARY_HEADER_WORKLOAD_NOTES = "Workload notes";
    @yield const SUMMARY_MAX_SIZE;
    @yield const SUMMARY_ITEMS;
}

output_consts_group! {
    const WORKLOAD_TASK_INFO_GROUP_1_THROUGHPUT_RAW = "Throughput (raw)";
    const WORKLOAD_TASK_INFO_GROUP_2_THROUGHPUT_FULL = "Throughput (full)";
    const WORKLOAD_TASK_INFO_GROUP_3_COUNT = "Queries executed";
    const WORKLOAD_TASK_INFO_GROUP_4_NOTES = "Description";
    @yield const WORKLOAD_TASK_INFO_GROUP_1_MAX;
    @yield const WORKLOAD_TASK_INFO_GROUP_1;
}

output_consts_group! {
    const WORKLOAD_TASK_LATENCY_1_MEAN = "mean";
    const WORKLOAD_TASK_LATENCY_2_MIN = "min";
    const WORKLOAD_TASK_LATENCY_3_MAX = "max";
    const WORKLOAD_TASK_LATENCY_4_STDEV = "stdev";
    @yield const WORKLOAD_TASK_LATENCY_GROUP_MAX;
    @yield const WORKLOAD_TASK_LATENCY_GROUP;
}

output_consts_group! {
    const WORKLOAD_TASK_LATENCY_DISTRIBUTION_1 = "Server";
    const WORKLOAD_TASK_LATENCY_DISTRIBUTION_2 = "Full";
    @yield const WORKLOAD_TASK_LATENCY_DISTRIBUTION_MAX;
    @yield const WORKLOAD_TASK_LATENCY_DISTRIBUTION;
}

impl ComprehensiveRuntimeStats {
    pub fn new(
        database_config: Box<str>,
        benchmark_tool_config: Box<str>,
        protocol: Box<str>,
        workload: Box<str>,
        mode: Box<str>,
        workload_notes: Option<Box<str>>,
        results: Vec<ComprehensiveWorkloadTaskStats>,
    ) -> Self {
        Self {
            database_config,
            benchmark_tool_config,
            protocol,
            workload,
            mode,
            workload_notes,
            task_results: results,
        }
    }
    pub fn display(self) {
        // summary
        for (summary_item, summary_data) in SUMMARY_ITEMS.into_iter().zip([
            self.database_config.as_ref(),
            self.benchmark_tool_config.as_ref(),
            self.protocol.as_ref(),
            self.workload.as_ref(),
            fmt_u64(self.task_results.iter().map(|wl| wl.executed).sum::<u64>()).as_ref(),
            self.mode.as_ref(),
        ]) {
            println!("{summary_item:SUMMARY_MAX_SIZE$} : {summary_data}");
        }
        if let Some(ref workload_notes) = self.workload_notes {
            println!("{SUMMARY_HEADER_WORKLOAD_NOTES:SUMMARY_MAX_SIZE$} : {workload_notes}")
        }
        println!();
        // workload info
        let workload_name_header_padding = self
            .task_results
            .iter()
            .map(|wl| wl.workload_name.len())
            .max()
            .unwrap()
            + self.task_results.len().to_string().len()
            + 1; // for the dot (.)
        let mut workload_tasks = self
            .task_results
            .into_iter()
            .enumerate()
            .map(|(i, d)| (i + 1, d))
            .peekable();
        while let Some((
            i,
            ComprehensiveWorkloadTaskStats {
                workload_name,
                notes,
                throughput_qps_full,
                throughput_qps_raw,
                executed,
                server_latency,
                full_latency,
                server_latency_distribution,
                full_latency_distribution,
            },
        )) = workload_tasks.next()
        {
            // workload info
            println!("{}", "-".repeat(workload_name_header_padding + 1));
            println!("{i}. {workload_name:workload_name_header_padding$}");
            println!("{}", "-".repeat(workload_name_header_padding + 1));
            // basic stats
            for (item, value) in WORKLOAD_TASK_INFO_GROUP_1.into_iter().zip([
                Some(fmt_f64_extra(throughput_qps_raw, 4, " queries/sec").into_boxed_str()),
                Some(fmt_f64_extra(throughput_qps_full, 4, " queries/sec").into_boxed_str()),
                Some(fmt_u64(executed).into_boxed_str()),
                notes,
            ]) {
                if let Some(value) = value {
                    println!("{item:WORKLOAD_TASK_INFO_GROUP_1_MAX$} : {value}");
                }
            }
            for (latency_group, latency_data) in [
                (
                    "Server latency",
                    [
                        server_latency.mean,
                        server_latency.min,
                        server_latency.max,
                        server_latency.stdev,
                    ],
                ),
                (
                    "Full latency",
                    [
                        full_latency.mean,
                        full_latency.min,
                        full_latency.max,
                        full_latency.stdev,
                    ],
                ),
            ] {
                // server latency
                println!("{latency_group}");
                for (item, value) in WORKLOAD_TASK_LATENCY_GROUP.into_iter().zip(latency_data) {
                    println!("    {item:WORKLOAD_TASK_LATENCY_GROUP_MAX$} (ms): {value}");
                }
            }
            println!("Latency distribution");
            for (distribution, latencies) in WORKLOAD_TASK_LATENCY_DISTRIBUTION
                .into_iter()
                .zip([server_latency_distribution, full_latency_distribution])
            {
                let mut latencies = latencies.into_iter().peekable();
                let mut formatted_distribution = String::new();
                while let Some((percentile, value)) = latencies.next() {
                    formatted_distribution.push_str(&format!("{percentile}% <= {value} ms"));
                    if latencies.peek().is_some() {
                        formatted_distribution.push_str(", ");
                    }
                }
                println!("    {distribution:WORKLOAD_TASK_LATENCY_DISTRIBUTION_MAX$}: {formatted_distribution}");
            }
            if workload_tasks.peek().is_some() {
                println!()
            } else {
                println!("\n==== END OF RESULTS ====")
            }
        }
    }
}
