#include <oneapi/tbb/concurrent_hash_map.h>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/tick_count.h>
#include <oneapi/tbb/tbb_allocator.h>
#include <oneapi/tbb/global_control.h>

#include <string>
#include <iostream>
#include <fstream>
#include <vector>

#include <benchmark/benchmark.h>

using namespace tbb;
using namespace std;

int num_lines = 195'841'983;
int n_categories = 26;










using hash_value_type = uint32_t;

// MurmurHash3_32 implementation from
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
// Note - The x86 and x64 versions do _not_ produce the same results, as the
// algorithms are optimized for their respective platforms. You can still
// compile and run any of them on any platform, but your performance with the
// non-native version will be less than optimal.

constexpr uint32_t rotl32(uint32_t x, int8_t r) 
{
    return (x << r) | (x >> (32 - r));
}

constexpr uint32_t fmix32(uint32_t h) 
{
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}


constexpr size_t mm_hash(uint32_t const& key) 
{
constexpr int len         = sizeof(uint32_t);
const uint8_t* const data = (const uint8_t*)&key;
constexpr int nblocks     = len / 4;

uint32_t m_seed = 0;
uint32_t h1           = m_seed;
constexpr uint32_t c1 = 0xcc9e2d51;
constexpr uint32_t c2 = 0x1b873593;
//----------
// body
const uint32_t* const blocks = (const uint32_t*)(data + nblocks * 4);
for (int i = -nblocks; i; i++) {
    uint32_t k1 = blocks[i];  // getblock32(blocks,i);
    k1 *= c1;
    k1 = rotl32(k1, 15);
    k1 *= c2;
    h1 ^= k1;
    h1 = rotl32(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
}
//----------
// tail
const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
uint32_t k1         = 0;
switch (len & 3) {
    case 3: k1 ^= tail[2] << 16;
    case 2: k1 ^= tail[1] << 8;
    case 1:
    k1 ^= tail[0];
    k1 *= c1;
    k1 = rotl32(k1, 15);
    k1 *= c2;
    h1 ^= k1;
};
//----------
// finalization
h1 ^= len;
h1 = fmix32(h1);
return h1;
}








 
// Structure that defines hashing and comparison operations for user's type.
struct MyHashCompare {
    static size_t hash( const uint32_t& x ) {
        return mm_hash(x);
    }
    //! True if strings are equal
    static bool equal( const uint32_t& x, const uint32_t& y ) {
        return x==y;
    }
};
 
// A concurrent hash table that maps strings to ints.
typedef concurrent_hash_map<int32_t,int,MyHashCompare> intTable;
 
// Function object for counting occurrences of strings.
struct Tally {
    intTable& table;
    Tally( intTable& table_ ) : table(table_) {}
    void operator()( const blocked_range<int32_t*> range ) const {
        for( int32_t* p=range.begin(); p!=range.end(); ++p ) {
            intTable::accessor a;
            table.insert( a, *p );
            a->second += 1;
        }
    }
};

template <typename Key>
float CountOccurrences(std::vector<Key> keys) {
    // Construct empty table.
    intTable table;

    oneapi::tbb::tick_count t0 = oneapi::tbb::tick_count::now();
 
    // Put occurrences into the table
    parallel_for( blocked_range<int32_t*>( keys.data(), keys.data() + num_lines, 1000 ),
                  Tally(table) );

    oneapi::tbb::tick_count t1 = oneapi::tbb::tick_count::now();

    return (t1-t0).seconds();
}









// broken: 9, 15, 19, 20, 21

static void gen_category(benchmark::internal::Benchmark* b) {
  for(auto idx = 22; idx < n_categories; ++idx) {
    b->Args({idx});
  }
}

template <typename Key, typename Value>
static void BM_tbb_insert(::benchmark::State& state) {

  int category = state.range(0);
  std::size_t num_keys = num_lines;

  std::vector<Key> keys( num_keys );

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
    keys[i] = stol(line, NULL, 16);
  }
  criteo_data.close();

    
  int num_threads = 12;
  oneapi::tbb::global_control c(oneapi::tbb::global_control::max_allowed_parallelism,
                                num_threads);


  for(auto _ : state) {
    float build_time = CountOccurrences(keys);
    state.SetIterationTime(build_time);
  }
}

BENCHMARK_TEMPLATE(BM_tbb_insert, int32_t, int32_t)
  ->Unit(benchmark::kMillisecond)
  ->Apply(gen_category)
  ->UseManualTime();