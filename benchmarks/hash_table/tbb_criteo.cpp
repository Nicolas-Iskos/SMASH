#include <oneapi/tbb/concurrent_hash_map.h>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/tick_count.h>
#include <oneapi/tbb/tbb_allocator.h>
#include <oneapi/tbb/global_control.h>

#include <string>
#include <iostream>
#include <fstream>

using namespace tbb;
using namespace std;
 
// Structure that defines hashing and comparison operations for user's type.
struct MyHashCompare {
    static size_t hash( const string& x ) {
        size_t h = 0;
        for( const char* s = x.c_str(); *s; ++s )
            h = (h*17)^*s;
        return h;
    }
    //! True if strings are equal
    static bool equal( const string& x, const string& y ) {
        return x==y;
    }
};
 
// A concurrent hash table that maps strings to ints.
typedef concurrent_hash_map<string,int,MyHashCompare> StringTable;
 
// Function object for counting occurrences of strings.
struct Tally {
    StringTable& table;
    Tally( StringTable& table_ ) : table(table_) {}
    void operator()( const blocked_range<string*> range ) const {
        for( string* p=range.begin(); p!=range.end(); ++p ) {
            StringTable::accessor a;
            table.insert( a, *p );
            a->second += 1;
        }
    }
};
 
const size_t N = 1000000;
 
string Data[N];
 
void CountOccurrences() {
    // Construct empty table.
    StringTable table;

    oneapi::tbb::tick_count t0 = oneapi::tbb::tick_count::now();
 
    // Put occurrences into the table
    parallel_for( blocked_range<string*>( Data, Data+N, 1000 ),
                  Tally(table) );

    oneapi::tbb::tick_count t1 = oneapi::tbb::tick_count::now();
    printf("time for action = %g mseconds\n", 1000 * (t1-t0).seconds() );
 
    // Display the occurrences
    //for( StringTable::iterator i=table.begin(); i!=table.end(); ++i )
        //printf("%s %d\n",i->first.c_str(),i->second);
}

void get_nth_category(string& line, string& output, int n) {
    const int start_offset = 14; // label + 13 non-categorical columns

    int occurrence = 0;
    int index = -1;
    while(occurrence - start_offset < n) {
        if((index = line.find("\t", index + 1)) != string::npos) {
            cout << "indexl: " << index << endl;
            occurrence++;
        }
    }

    int start_pos = index + 1;
    cout << "start_pos: " << start_pos << endl;
    int end_pos = line.find("\t", index + 1);
    cout << "end_pos: " << end_pos << endl;

    output = line.substr(start_pos, end_pos - start_pos);
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
   

    string line;
    getline(criteo_data, line);
    //getline(criteo_data, line);

    string output;
    get_nth_category(line, output, 0);

    cout << "line: " << line << endl;
    cout << "output: " << output << endl;



    criteo_data.close();    //close the file object
}

int main() {

    process_data();

    int num_threads = 12;
    oneapi::tbb::global_control c(oneapi::tbb::global_control::max_allowed_parallelism,
                                          num_threads);

    CountOccurrences();

    return 0;
}