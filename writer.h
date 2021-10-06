//
// Created by ruslan on 10/4/21.
//

#ifndef SHUFFLE_WRITER_H
#define SHUFFLE_WRITER_H

#include <vector>
#include <ostream>
#include <queue>
#include <thread>
#include <unordered_map>
#include <condition_variable>
#include <atomic>
#include <memory>
#include "common.h"

class ShuffleWriter {
public:
    ShuffleWriter(int n_partitions, int num_cores, std::ostream &os);

    void write(part_type part_id, const std::vector<byte> &bytes);

    void done();

    void wait_for_completion();

    void flush(part_type part_id);

    int64_t total_bytes() { return _total_bytes; }

private:
    // launches task threads which
    // wait on conditional variable and push data to the network
    // num_tasks needs to be proportional (or equal) to the number of cores
    void launch_tasks(int num_tasks = 4);

    static void task_thread(ShuffleWriter &sw);

    std::mutex &get_mutex(int part_id);

    int _n_partitions;
    int _num_cores;

    // pretend that this is network socket
    std::ostream &_os;

    // array of queues for individual partition buffers
    std::vector<std::queue<std::vector<byte>>> _qs;

    // synchronization array, each lock per partition queue
    std::vector<std::unique_ptr<std::mutex>> _locks;

    // working buffers, one per each partition queue
    // max size MAX_WORK_BUF
    std::vector<std::vector<byte>> _wbs;

    // indicates that data is available
    std::condition_variable _has_data;
    // queue to indicate available data for a partition id
    std::queue<part_type> _ids;
    // mutex for id queue
    std::mutex _mx_ids;

    // if done, finish writing data to network
    // then shutdown
    std::atomic<bool> _done;

    // worker threads;
    std::vector<std::thread> _workers;

    std::atomic<int64_t> _total_bytes;

};

#endif //SHUFFLE_WRITER_H
