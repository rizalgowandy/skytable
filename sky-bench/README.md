# Skytable Benchmark Tool

Skytable's benchmark tool `sky-bench` is a fairly advanced load-testing tool that can be used to analyze the performance of Skytable installations, and its performance on a certain network with support for full spectrum latency analysis. One of the primary goals of this tool is to provide **real-world like workloads** and hence as part of the throughput report we output raw throughput and the parsing and validation overhead that you would incur in the real-world when you use Skytable in your applications.

The core of this utility is relatively simple: a thread-pool is created on top of which multiple client "tasks" execute queries. For each query, multiple statistics are computed including:

- the server latency (sampled for each query) which is the time taken for a response from the server to first reach the client
- the full latency (sampled for each query) which is the total time taken for a full response to be received from the server

## Workloads

A workload based approach is used for benchmarking. The currently supported workload is `uniform_std_v1` (see the benchmark output for more details on what is executed). Overall, multiple unique rows (1,000,000 unique rows by default) are created with `INSERT`, manipulated with an `UPDATE`, fetched with a `SELECT` and finally deleted with a `DELETE`. Hence, for each unique row 4 queries are executed; this means a total of 4,000,000 queries are executed by default.

The workload can be selected using `--workload {workload_name}`.

## Engine-based *(deprecated)*

Previously an engine-based approach was used which is now deprecated due to lack of extensibility and other limitations with comprehensive execution analysis. The engines available are `rookie` and `fury`. The engine can be selected using `--engine {engine_name}`.

Please note that we **do not recommend using these options and recommend you to use workloads instead**.
