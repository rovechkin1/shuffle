## Simple shuffler for multiple partitons
Only implements writer part

Writer allows writes to multiple in-memory partitions. Each partition is 
guarded by individual mutex. Writes are first done in work buffer when buffer reached 8 MB size the data is
compressed and sent over the wire. Each write is an individual record and written to a buffer
as 
```
record := <4-byte len header> | <bytes>
```

Similarly when data is compressed, it is written as 
```
chunk: = <size> | <compressed bytes>
```

Data is sent over the wire using a set of worker threads. The number of threads
needs to be proportional to the number of cores for optimal performance.
Each worker is waiting on conditional variable (_has_data), which is signalled when new compressed
chunk becomes available. To indicate which partition has a chunk available for transmissing, there is a
separate queue (_ids). When writer finishes chunk it signals to one of the threads that data is available.
A thread will pick up a chunk and write it to the wire (ostream). If there is more data available it will
notify other threads via the same conditional variable (_has_data).

Writer also implements a cancellation logic to shutdown threads.

It is also possible to introduce mmap to be able to swap chunk on disk. It can be either done via 
custom memory-mapped allocator which can be used with std::queue, or custom circular FIFO queue can be implemented which can use 
mmap directly. Circular FIFO queue would also improve performance without mmap, since
no allocation/deallocation is required for push/pop.

