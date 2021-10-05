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

class ShuffleWriter{
public:
    ShuffleWriter(int n_partitions, std::ostream& os);
    void write(part_type part_id, const std::vector<byte>& bytes);
    void cancel();
    void wait_for_completion();
    void flush(part_type part_id);
private:
    // launches task threads which
    // wait on conditional variable and push data to the network
    // num_tasks needs to be proportional (or equal) to the number of cores
    void launch_tasks(int num_tasks=4);

    static void task_thread(ShuffleWriter& sw) ;

    // pretend that this is network socket
    std::ostream& _os;

    // map of queues for individual partition buffers
    std::unordered_map<part_type,std::queue<std::vector<byte>>> _qs;

    // synchronization map, each lock per partition queue
    std::unordered_map<part_type,std::unique_ptr<std::mutex>> _locks;

    // working buffers, one per each partition queue
    // max size MAX_WORK_BUF
    std::unordered_map<part_type,std::vector<byte>> _wbs;

    // indicates that data is available
    std::condition_variable _has_data;
    // queue to indicate available data for a partition id
    std::queue<part_type> _ids;
    // mutex for id queue
    std::mutex _mx_ids;

    // if cancelled
    std::atomic<bool> _cancelled;

    // worker threads;
    std::vector<std::thread> _workers;

};

#endif //SHUFFLE_WRITER_H
