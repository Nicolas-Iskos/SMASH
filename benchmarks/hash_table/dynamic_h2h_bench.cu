/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <SlabHashResizing/src/gpu_hash_table.cuh>
#include <cuco/legacy_static_map.cuh>
#include <single_value_hash_table.cuh>

#include <benchmark/benchmark.h>
#include <synchronization.hpp>
#include <cuco/dynamic_map.cuh>
#include <iostream>
#include <random>

enum class dist_type {
  UNIQUE,
  UNIQUE_NONE,
  UNIFORM,
  GAUSSIAN
};

template<dist_type Dist, typename Key, typename OutputIt>
static void generate_keys(OutputIt output_begin, OutputIt output_end) {
  auto num_keys = std::distance(output_begin, output_end);
  
  std::random_device rd;
  std::mt19937 gen{rd()};

  switch(Dist) {
    case dist_type::UNIQUE:
      for(auto i = 0; i < num_keys; ++i) {
        output_begin[i] = i;
      }
      shuffle(output_begin, output_end, std::default_random_engine(10));
      break;
    case dist_type::UNIQUE_NONE:
      for(auto i = 0; i < num_keys; ++i) {
        output_begin[i] = i + num_keys;
      }
      shuffle(output_begin, output_end, std::default_random_engine(10));
      break;
    case dist_type::UNIFORM:
      for(auto i = 0; i < num_keys; ++i) {
        output_begin[i] = std::abs(static_cast<Key>(gen()));
      }
      break;
    case dist_type::GAUSSIAN:
      std::normal_distribution<> dg{1e9, 4e7};
      for(auto i = 0; i < num_keys; ++i) {
        output_begin[i] = std::abs(static_cast<Key>(dg(gen)));
      }
      break;
  }
}

static void gen_final_size(benchmark::internal::Benchmark* b) {
  for(auto size = 10'000'000; size <= 310'000'000; size += 20'000'000) {
    b->Args({size});
  }
}

static constexpr uint32_t get_num_super_blocks(
  uint32_t log_num_mem_blocks,
  uint32_t total_footprint,
  float split_fraction) {
  
  uint32_t slab_size = 128;
  uint32_t pool_slabs_size = (1 - split_fraction) * total_footprint;
  uint32_t mem_per_super_block = (1<<10) * slab_size * (1<<log_num_mem_blocks);
  uint32_t num_super_blocks = 
    (pool_slabs_size + mem_per_super_block - 1) / mem_per_super_block;

  return num_super_blocks;
}

template <typename Key, typename Value, dist_type Dist>
static void BM_dynamic_insert(::benchmark::State& state) {

  using map_type = cuco::dynamic_map<Key, Value,
                                     cuda::thread_scope_device,
                                     cuco::legacy_static_map>;
  
  std::size_t num_keys = state.range(0);
  std::size_t initial_size = 1<<27;
  
  std::vector<Key> h_keys( num_keys );
  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );
  
  generate_keys<Dist, Key>(h_keys.begin(), h_keys.end());

  for(auto i = 0; i < num_keys; ++i) {
    Key key = h_keys[i];
    Value val = h_keys[i];
    h_pairs[i].first = key;
    h_pairs[i].second = val;
  }

  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );

  std::size_t batch_size = 1E7;
  for(auto _ : state) {
    map_type map{initial_size, -1, -1};
    {
      cuda_event_timer raii{state}; 
      for(auto i = 0; i < num_keys; i += batch_size) {
        map.insert(d_pairs.begin() + i, d_pairs.begin() + i + batch_size);
      }
    }
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

template <typename Key, typename Value, dist_type Dist>
static void BM_dynamic_find_all(::benchmark::State& state) {
  using map_type = cuco::dynamic_map<Key, Value, 
                                     cuda::thread_scope_device, 
                                     cuco::legacy_static_map>;
  
  std::size_t num_keys = state.range(0);
  std::size_t initial_size = 1<<27;

  std::vector<Key> h_keys( num_keys );
  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );

  generate_keys<Dist, Key>(h_keys.begin(), h_keys.end());
  
  for(auto i = 0; i < num_keys; ++i) {
    Key key = h_keys[i];
    Value val = h_keys[i];
    h_pairs[i].first = key;
    h_pairs[i].second = val;
  }

  thrust::device_vector<Key> d_keys( h_keys );
  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );
  thrust::device_vector<Value> d_results( num_keys );

  std::size_t batch_size = 1E7;
  map_type map{initial_size, -1, -1};
  for(auto i = 0; i < num_keys; i += batch_size) {
    map.insert(d_pairs.begin() + i, d_pairs.begin() + i + batch_size);
  }

  for(auto _ : state) {
    cuda_event_timer raii{state};
    map.find(d_keys.begin(), d_keys.end(), d_results.begin());
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

template <typename Key, typename Value, dist_type Dist>
static void BM_dynamic_find_none(::benchmark::State& state) {
  using map_type = cuco::dynamic_map<Key, Value, 
                                     cuda::thread_scope_device, 
                                     cuco::legacy_static_map>;
  
  std::size_t num_keys = state.range(0);
  std::size_t initial_size = 1<<27;

  std::vector<Key> h_keys( num_keys );
  std::vector<Key> h_search_keys( num_keys );
  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );

  generate_keys<Dist, Key>(h_keys.begin(), h_keys.end());
  generate_keys<dist_type::UNIQUE_NONE, Key>(h_search_keys.begin(), h_search_keys.end());

  for(auto i = 0; i < num_keys; ++i) {
    Key key = h_keys[i];
    Value val = h_keys[i];
    h_pairs[i].first = key;
    h_pairs[i].second = val;
  }

  thrust::device_vector<Key> d_search_keys( h_search_keys );
  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );
  thrust::device_vector<Value> d_results( num_keys );

  std::size_t batch_size = 1E7;
  map_type map{initial_size, -1, -1};
  for(auto i = 0; i < num_keys; i += batch_size) {
    map.insert(d_pairs.begin() + i, d_pairs.begin() + i + batch_size);
  }

  for(auto _ : state) {
    cuda_event_timer raii{state};
    map.find(d_search_keys.begin(), d_search_keys.end(), d_results.begin());
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

template <typename Key, typename Value, dist_type Dist>
static void BM_slabhash_insert(::benchmark::State& state) {

  std::size_t num_keys = state.range(0);

  // Configure these three parameters for the particular benchmark
  /*********************************************/
  constexpr std::size_t total_footprint = 1<<30;
  constexpr float split_fraction = 0.3125;
  constexpr float thresh_lf = 0.75;
  /*********************************************/

  constexpr std::size_t base_slabs_size = split_fraction * total_footprint;
  constexpr std::size_t slab_size = 128;
  constexpr std::size_t num_buckets = base_slabs_size / slab_size;
  
  constexpr uint32_t log_num_mem_blocks = 9; // 64 MiB per super block
  constexpr uint32_t num_super_blocks = get_num_super_blocks(log_num_mem_blocks, 
    total_footprint, split_fraction);

  int64_t device_idx = 0;
  int64_t seed = 12;

  using map_type = gpu_hash_table<Key, Value, SlabHashTypeT::ConcurrentMap, 
                                  log_num_mem_blocks, num_super_blocks>;
  
  std::vector<Key> h_keys( num_keys );
  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );
  
  generate_keys<Dist, Key>(h_keys.begin(), h_keys.end());
  std::vector<Value> h_values (h_keys);

  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );

  std::size_t batch_size = 1E7;
  for(auto _ : state) {
    auto build_time = 0.0f;
    map_type map{batch_size, num_buckets, device_idx, seed, true, false, false, thresh_lf};
    for(uint32_t i = 0; i < num_keys; i += batch_size) {
      build_time += map.hash_build_with_unique_keys(h_keys.data() + i, 
                                                    h_values.data() + i, batch_size);
    }
    state.SetIterationTime((float)build_time / 1000);
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

template <typename Key, typename Value, dist_type Dist>
static void BM_slabhash_find_all(::benchmark::State& state) {

  std::size_t num_keys = state.range(0);
  
  // Configure these three parameters for the particular benchmark
  /*********************************************/
  constexpr std::size_t total_footprint = 1<<30;
  constexpr float split_fraction = 0.3125;
  constexpr float thresh_lf = 0.75;
  /*********************************************/

  constexpr std::size_t base_slabs_size = split_fraction * total_footprint;
  constexpr std::size_t slab_size = 128;
  constexpr std::size_t num_buckets = base_slabs_size / slab_size;
  
  constexpr uint32_t log_num_mem_blocks = 9; // 64 MiB per super block
  constexpr uint32_t num_super_blocks = get_num_super_blocks(log_num_mem_blocks, 
    total_footprint, split_fraction);
 
  int64_t device_idx = 0;
  int64_t seed = 12;
  
  using map_type = gpu_hash_table<Key, Value, SlabHashTypeT::ConcurrentMap, 
                                  log_num_mem_blocks, num_super_blocks>;
  
  std::vector<Key> h_keys( num_keys );
  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );
  
  generate_keys<Dist, Key>(h_keys.begin(), h_keys.end());
  std::vector<Value> h_values (h_keys);
  std::vector<Value> h_results(num_keys);

  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );

  std::size_t batch_size = 1E7;
  map_type map{num_keys, num_buckets, device_idx, seed, true, false, false, thresh_lf};
  for(uint32_t i = 0; i < num_keys; i += batch_size) {
    map.hash_build_with_unique_keys(h_keys.data() + i, 
                                    h_values.data() + i, batch_size);
  }
  
  for(auto _ : state) {
    auto find_time = 0.0f;
    find_time = map.hash_search(h_keys.data(), h_results.data(), num_keys);
    state.SetIterationTime((float)find_time / 1000);
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

template <typename Key, typename Value, dist_type Dist>
static void BM_slabhash_find_none(::benchmark::State& state) {

  std::size_t num_keys = state.range(0);
  
  // Configure these three parameters for the particular benchmark
  /*********************************************/
  constexpr std::size_t total_footprint = 1<<30;
  constexpr float split_fraction = 0.0625;
  constexpr float thresh_lf = 0.90;
  /*********************************************/

  constexpr std::size_t base_slabs_size = split_fraction * total_footprint;
  constexpr std::size_t slab_size = 128;
  constexpr std::size_t num_buckets = base_slabs_size / slab_size;
  
  constexpr uint32_t log_num_mem_blocks = 9; // 64 MiB per super block
  constexpr uint32_t num_super_blocks = get_num_super_blocks(log_num_mem_blocks, 
    total_footprint, split_fraction);
  
  int64_t device_idx = 0;
  int64_t seed = 12;
  
  using map_type = gpu_hash_table<Key, Value, SlabHashTypeT::ConcurrentMap, 
                                  log_num_mem_blocks, num_super_blocks>;
  
  std::vector<Key> h_keys( num_keys );
  std::vector<Key> h_search_keys( num_keys );
  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );
  
  generate_keys<Dist, Key>(h_keys.begin(), h_keys.end());
  generate_keys<dist_type::UNIQUE_NONE, Key>(h_search_keys.begin(), h_search_keys.end());
  std::vector<Value> h_values (h_keys);
  std::vector<Value> h_results(num_keys);

  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );

  std::size_t batch_size = 1E7;
  map_type map{num_keys, num_buckets, device_idx, seed, true, false, false, thresh_lf};
  for(uint32_t i = 0; i < num_keys; i += batch_size) {
    map.hash_build_with_unique_keys(h_keys.data() + i, 
                                    h_values.data() + i, batch_size);
  }
  
  for(auto _ : state) {
    auto find_time = 0.0f;
    find_time = map.hash_search(h_search_keys.data(), h_results.data(), num_keys);
    state.SetIterationTime((float)find_time / 1000);
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

/*
BENCHMARK_TEMPLATE(BM_dynamic_insert, int32_t, int32_t, dist_type::UNIQUE)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_final_size)
  ->UseManualTime();
//*/

/*
BENCHMARK_TEMPLATE(BM_dynamic_find_all, int32_t, int32_t, dist_type::UNIQUE)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_final_size)
  ->UseManualTime();
//*/

/*
BENCHMARK_TEMPLATE(BM_dynamic_find_all, int32_t, int32_t, dist_type::UNIQUE)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_final_size)
  ->UseManualTime();
//*/

//*
BENCHMARK_TEMPLATE(BM_slabhash_insert, int32_t, int32_t, dist_type::UNIQUE)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_final_size)
  ->UseManualTime();
//*/

//*
BENCHMARK_TEMPLATE(BM_slabhash_find_all, int32_t, int32_t, dist_type::UNIQUE)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_final_size)
  ->UseManualTime();
//*/

//*
BENCHMARK_TEMPLATE(BM_slabhash_find_none, int32_t, int32_t, dist_type::UNIQUE)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_final_size)
  ->UseManualTime();
//*/