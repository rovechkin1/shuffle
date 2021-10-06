#include <iostream>
#include <sstream>
#include <unistd.h>
#include <chrono>
#include "writer.h"

int main(int argc, char **argv) {
    int num_part = 100;
    int num_iter = 100;
    int num_cores = 4;
    for (int i = 1; i < argc;) {
        if (std::string(argv[i]) == "--help" ||
            std::string(argv[i]) == "-h") {
            std::cout << "Usage: shuffle -p <partitions|100> -i <iterations|100> -c <cores|4>" << std::endl;
            return 0;
        } else if (std::string(argv[i]) == "-p" && i + 1 < argc) {
            std::stringstream ss(argv[i + 1]);
            ss >> num_part;
            i += 2;
        } else if (std::string(argv[i]) == "-i" && i + 1 < argc) {
            std::stringstream ss(argv[i + 1]);
            ss >> num_iter;
            i += 2;
        } else if (std::string(argv[i]) == "-c" && i + 1 < argc) {
            std::stringstream ss(argv[i + 1]);
            ss >> num_cores;
            i += 2;
        } else {
            std::cout << "Invalid argument: " << argv[i] << std::endl;
            return 1;
        }
    }

    ShuffleWriter sw(num_part, num_cores,std::cout);
    auto t_start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> workers;
    for (int i = 0; i < num_part; ++i) {
        workers.emplace_back(std::thread([i, &sw, num_iter]() {
            //std::cout<<"worker: "<<i<<std::endl;
            for (int j = 0; j < num_iter; ++j) {
                std::stringstream ss;
                ss << "worker: " << i << " j:" << j;
                std::string s = ss.str();
                sw.write(i, std::vector<byte>(s.begin(), s.end()));
            }
            sw.flush(i);
        }));
    }
    for (auto &w: workers) {
        w.join();
    }
    sw.done();
    sw.wait_for_completion();
    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    std::cout << "done, num_part: " << num_part
              << ", num iter: " << num_iter << ", elapsed time: "
              << elapsed_time_ms << ", ms" << std::endl;
    std::cout << "total bytes :" << sw.total_bytes() << std::endl;
    return 0;
}
