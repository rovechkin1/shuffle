#include <iostream>
#include <sstream>
#include <unistd.h>
#include "writer.h"

int main() {
    // mock reader
    int num_part = 100;
    ShuffleWriter sw(num_part, std::cout);

    std::vector<std::thread> workers;
    for (int i = 0; i < num_part; ++i) {
        workers.emplace_back(std::thread([i, &sw]() {
            //std::cout<<"worker: "<<i<<std::endl;
            for (int j = 0; j < 100; ++j) {
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
    sleep(5);
    sw.cancel();
    sw.wait_for_completion();
    std::cout << "done" << std::endl;
    std::cout << "total bytes :" << sw.total_bytes() << std::endl;
    return 0;
}
