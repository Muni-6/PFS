#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>

#include "pfs_common/pfs_config.hpp"

#ifndef PFS_CACHE_HPP
#define PFS_CACHE_HPP

#include <unordered_map>
#include <vector>
#include <list>
#include <mutex>
#include <ctime>
#include <cstring>
#include <string>

// Constants

class PFSCache {
private:
    struct CacheBlock {
        std::vector<char> data;       // Data buffer
        bool is_dirty;                // Indicates if the block has been modified
        std::int32_t block_number;    // Block number within the file
        std::string filename;         // File to which this block belongs
        std::time_t last_access;      // Last access time for LRU eviction

        CacheBlock() : is_dirty(false), block_number(-1), last_access(0) {}
    };

struct pfs_execstat& global_cache_stat;
    size_t max_blocks; // Maximum number of cache blocks allowed
    std::unordered_map<std::string, std::unordered_map<int32_t, CacheBlock>> cache; // Cache data structure
    std::mutex cache_mutex; // Mutex for thread safety

    size_t getCacheSize(); // Helper function to get the current cache size
    void evictBlock();     // Helper function to evict the least recently used block
    void writeBackBlock(const std::string& filename, int32_t block_num, const void* data, size_t size);

public:
    explicit  PFSCache(size_t max_cache_blocks, struct pfs_execstat& global_cache_stat);

    bool isBlockCached(const std::string& filename, int32_t block_num);
    int readFromCache(const std::string& filename, int32_t block_num, void* buf, size_t block_offset, size_t bytes_to_read);
    int writeToCache(const std::string& filename, int32_t block_num, const void* buf, off_t offset, size_t num_bytes);
    void addToCache(const std::string& filename, int32_t block_num, const void* data, size_t size);
    void removeFromCache(const std::string& filename, int32_t block_num);
    bool isDirty(const std::string& filename, int32_t block_num);
    void markDirty(const std::string& filename, int32_t block_num);
    bool getBlock(const std::string& filename, int32_t block_num, void** data, size_t* size);
    void WriteBackAllFileName(std::string& file_name);
    void WriteBackAllAndClear();
};

#endif // PFS_CACHE_HPP
