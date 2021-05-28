#include <cuco/static_reduction_map.cuh>
#include <benchmark/benchmark.h>
#include <synchronization.hpp>
#include <cuco/dynamic_map.cuh>
#include <iostream>
#include <random>
#include <string>
#include <fstream>

using std::string;
using std::fstream;
using std::cout;
using std::endl;
using std::ios;

int num_lines = 195'841'983;
int n_categories = 26;
uint32_t *int_data;

static void gen_category(benchmark::internal::Benchmark* b) {
  for(auto idx = 0; idx < n_categories; ++idx) {
    b->Args({idx});
  }
}

void get_nth_category(string& line, string& output, int n) {
    const int start_offset = 14; // label + 13 non-categorical columns

    int occurrence = 0;
    int index = -1;
    while(occurrence - start_offset < n) {
        if((index = line.find("\t", index + 1)) != string::npos) {
            occurrence++;
        }
    }

    int start_pos = index + 1;
    //cout << "start_pos: " << start_pos << " " << line[start_pos] << endl;
    int end_pos = line.find("\t", start_pos + 1);
    //cout << "end_pos: " << end_pos << endl;

    output = line.substr(start_pos, end_pos - start_pos);
    //output.append(1, '\0');
}

void process_data() {

    fstream criteo_data;
    criteo_data.open("/home/nico/Documents/day_0", ios::in);
    if(criteo_data.is_open()) //checking whether the file is open
    {
       cout << "File successfully opened..." << endl;
       
    }
    else {
        cout << "File not opened!" << endl;
    }

    int_data = new uint32_t[num_lines];
    
    string line;
    for(auto i = 0; i < num_lines; ++i) {
        getline(criteo_data, line);
        string output;
        get_nth_category(line, output, 0);
        int_data[i] = stol(output, NULL, 16);
    }

    cout << std::hex << int_data[0] << " " << int_data[1] << " " << int_data[2] << endl;    
  
    criteo_data.close();    //close the file object
}

template <typename Key, typename Value>
static void BM_dynamic_insert(::benchmark::State& state) {

  using map_type = cuco::dynamic_map<cuco::reduce_add<Value>,
                                     Key, Value,
                                     cuda::thread_scope_device,
                                     cuco::cuda_allocator<char>,
                                     cuco::static_reduction_map>;
  
  int category = state.range(0);
  std::size_t num_keys = num_lines;

  // initial size 4 MB
  std::size_t initial_size = 1<<22;
  
  std::vector<Key> h_keys( num_keys );

  fstream criteo_data;
  criteo_data.open("/home/nico/Documents/category_" + std::to_string(category));
  if(criteo_data.is_open()) {
    cout << "File successfully opened..." << endl;
  }
  else {
    cout << "File not opened!" << endl;
  }

  // read file data into key buffer
  for(auto i = 0; i < num_keys; ++i) {
    string line;
    getline(criteo_data, line);
    h_keys[i] = stol(line, NULL, 16);
  }
  criteo_data.close();

  std::vector<cuco::pair_type<Key, Value>> h_pairs ( num_keys );
  
  for(auto i = 0; i < num_keys; ++i) {
    Key key = h_keys[i];
    Value val = h_keys[i];
    h_pairs[i].first = key;
    h_pairs[i].second = 1;
  }

  thrust::device_vector<cuco::pair_type<Key, Value>> d_pairs( h_pairs );

  std::size_t batch_size = 1E6;
  for(auto _ : state) {
    map_type map{initial_size, -1, -1};
    {
      cuda_event_timer raii{state};
      std::size_t num_remaining = num_keys;
      std::size_t insert_size = 0;
      for(auto i = 0; i < num_keys; i += insert_size) {
        insert_size = min(batch_size, num_remaining);
        map.insert(d_pairs.begin() + i, d_pairs.begin() + i + insert_size);
        num_remaining -= insert_size;
      }
    }

    std::cout << "map size: " << map.get_size() << std::endl;
  }

  state.SetBytesProcessed((sizeof(Key) + sizeof(Value)) *
                          int64_t(state.iterations()) *
                          int64_t(state.range(0)));
}

BENCHMARK_TEMPLATE(BM_dynamic_insert, int32_t, int32_t)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_category)
  ->UseManualTime();