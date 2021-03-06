#include <thread>
#include <sstream>
#include <iostream>
#include "writer.h"
#include "mock_snappy.h"

ShuffleWriter::ShuffleWriter(int n_partitions, int num_cores, std::ostream &os) :
        _n_partitions(n_partitions),
        _num_cores(num_cores),
        _os(os),
        _done(false),
        _total_bytes(0) {
    // initialize all arrays
    _qs = std::vector<std::queue<std::vector<byte>>>(n_partitions);
    _wbs = std::vector<std::vector<byte>>(n_partitions);
    for (int i = 0; i < n_partitions; ++i) {
        _locks.emplace_back(std::make_unique<std::mutex>());
    }

    launch_tasks(_num_cores);
}

void ShuffleWriter::write(part_type part_id, const std::vector<byte> &bytes) {
    if (_done) {
        throw std::runtime_error("writers are shutting down");
    }
    if (bytes.size() + sizeof(int) > MAX_WORK_BUF) {
        std::stringstream err;
        err << "message is too big: " << bytes.size() + sizeof(int) << " allowed: " << MAX_WORK_BUF;
        throw std::runtime_error(err.str());
    }

    auto &mx = get_mutex(part_id);

    // scoped mutex
    bool if_notify = false;
    {
        std::lock_guard<std::mutex> lock(mx);
        auto &w_buf = _wbs[part_id];
        if (MAX_WORK_BUF - (int) w_buf.size() - MSG_HEADER_LEN < (int) bytes.size()) {
            // compress and push it into the queue
            auto compressed = SnappyMock::compress(w_buf);
            _qs[part_id].emplace(compressed);
            w_buf.clear();

            // need to notify consumers that data is available
            if_notify = true;
        }

        // add header
        unsigned int sz = bytes.size();
        w_buf.insert(w_buf.end(), &sz, &sz + sizeof(sz));

        // copy message
        w_buf.insert(w_buf.end(), bytes.begin(), bytes.end());
    }

    if (if_notify) {
        // notify consumers that thre is data for this ppartition_id
        std::lock_guard<std::mutex> lock(_mx_ids);
        _ids.push(part_id);
        _has_data.notify_one();
    }
}

std::mutex &ShuffleWriter::get_mutex(int part_id) {
    if (part_id >= _n_partitions) {
        throw std::runtime_error("partition out of bounds");
    }
    return *_locks[part_id];
}

void ShuffleWriter::launch_tasks(int num_tasks) {
    // num_tasks needs to be proportional (or equal) to the number of cores
    for (int i = 0; i < num_tasks; ++i) {
        _workers.emplace_back(std::thread([&]() { task_thread(*this); }));
    }
}

void ShuffleWriter::done() {
    if (!_done) {
        _done = true;
        _has_data.notify_all();
    }
}

void ShuffleWriter::wait_for_completion() {
    for (auto &w: _workers) {
        w.join();
    }
}

// flushes buffer to network
// requires refactoring since it is mostly code copied from write
void ShuffleWriter::flush(part_type part_id) {
    if (_done) {
        throw std::runtime_error("writers shut down");
    }

    auto &mx = get_mutex(part_id);
    // scoped mutex
    {
        std::lock_guard<std::mutex> lock(mx);
        auto &w_buf = _wbs[part_id];
        // compress and push it into the queue
        auto compressed = SnappyMock::compress(w_buf);
        _qs[part_id].emplace(compressed);
        w_buf.clear();
    }

    // notify consumers that thre is data for this ppartition_id
    std::lock_guard<std::mutex> lock(_mx_ids);
    _ids.push(part_id);
    _has_data.notify_one();
}

void ShuffleWriter::task_thread(ShuffleWriter &sw) {
    while (true) {
        int part_id = -1;
        std::unique_lock<std::mutex> lk(sw._mx_ids);
        sw._has_data.wait(lk, [&] { return !sw._ids.empty() || sw._done; });

        // if done is signalled and no more data, return
        if (sw._done && sw._ids.empty()) {
            lk.unlock();
            return;
        }
        part_id = sw._ids.front();
        sw._ids.pop();
        // notify other threads that there are items in the queue
        bool if_notify = !sw._ids.empty();
        lk.unlock();
        if (if_notify) {
            sw._has_data.notify_one();
        }

        // process item from queue
        if (part_id >= sw._n_partitions) {
            // log error, ignore this part_id
            continue;
        }
        auto &mx = sw.get_mutex(part_id);
        // scoped mutex
        std::vector<byte> chunk;
        {
            std::lock_guard<std::mutex> lock(mx);
            auto &chunks = sw._qs[part_id];
            if (chunks.empty()) {
                // must not be empty, put assert here
                continue;
            }
            chunk = chunks.front(); // copy data
            chunks.pop();
        }

        // lock is released can write data to network
//        for (int i=0;i<chunk.size();++i){
//            sw._os<<chunk[i];
//        }
        // count total bytes
        sw._total_bytes += (int64_t) chunk.size();
    }

}


