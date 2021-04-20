# SMASH

This is the source code and benchmarking repository for SMASH: A GPU-Accelerated hash map

The production version of SMASH is the `cuco/dynamic_map` of NVIDIA's cuCollections library. This repository contains the source code for a slightly-modified version of the cuco/dynamic_map along with benchmarking code that is used to compare SMASH to other GPU-accelerated hash map implementations. For real-world applications, we recommend using the production `cuco/dynamic_map` from cuCollections.

The primary modifications to `cuco/dynamic_map` for this repository are: 
1. The use of legacy CUDA atomic operations such as `atomicCAS` to perform key/value insertions. By contrast, the production `cuco/dynamic_map` uses libcu++ atomics, which offer greater versatility for different sizes and types of key-value pairs at the expense of slightly decreased throughput.
2. The use of load-factor based heuristics to toggle between two different coalesced group sizes for probing operations for both insertion and search.

Static and dynamic benchmarks are contained in the files `benchmarks/static_h2h_bench.cu` and `benchmarks/dynamic_h2h_bench.cu`, respectively. Benchmark parameters can be selected within these files. To build the repository and run the benchmarks, perform the following steps.
1. `git submodule update --init --recursive`
2. `mkdir build && cd build`
3. `cmake ..'
4. `make'
5. `cd gbenchmarks'
6. `./STATIC_H2H_BENCH` or `./DYNAMIC_H2H_BENCH`.
