#include <string>
#include <iostream>
#include <fstream>
#include <vector>

using namespace std;

int num_lines = 195'841'983;

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
    int end_pos = line.find("\t", start_pos + 1);
    output = line.substr(start_pos, end_pos - start_pos);
}

void create_category_files() {
    // open criteo data
    const int n_categories = 26;
    fstream criteo_data;
    criteo_data.open("/home/nico/Documents/day_0", ios::in);
    
    vector<ofstream> outfiles(n_categories);
    // create category files
    for(auto i = 0; i < n_categories; ++i) {
        string fname = "/home/nico/Documents/category_";
        fname.append(to_string(i));
        new (outfiles.data() + i) ofstream(fname);
    }

    // fill category files
    for(auto i = 0; i < num_lines; ++i) {
        string line;
        getline(criteo_data, line);
        for(auto j = 0; j < n_categories; ++j) {
            string output;
            get_nth_category(line, output, j);
            outfiles[j] << output << endl;
        }
    }
}

int main() {
    create_category_files();
    return 0;
}