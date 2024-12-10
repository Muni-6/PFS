#include "pfs_cache.hpp"

#include <unordered_map>
#include <list>
#include <mutex>
#include <vector>
#include <cstring>
#include <ostream>
#include <iostream>
// #include "pfs_config.hpp"

// // In pfs_cache.hpp
// class PFSCache {
// private:
//     struct CacheBlock {
//         std::vector<char> data;
//         bool is_dirty;
//         std::int32_t block_number;
//         std::string filename;
//         std::time_t last_access;

//         CacheBlock() : is_dirty(false), block_number(-1), last_access(0) {}
//     };

//     size_t max_blocks;
//     std::unordered_map<std::string, std::unordered_map<int32_t, CacheBlock>> cache; // filename -> {block_num -> block}
//     std::mutex cache_mutex;

// public:
PFSCache::PFSCache(size_t max_cache_blocks) : max_blocks(max_cache_blocks) {}

bool PFSCache::isBlockCached(const std::string &filename, int32_t block_num)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto it = cache.find(filename);
    if (it != cache.end())
    {
        return it->second.find(block_num) != it->second.end();
    }
    return false;
}

// int readFromCache(const std::string& filename, void* buf, size_t num_bytes, off_t offset) {
//     std::lock_guard<std::mutex> lock(cache_mutex);

//     int32_t start_block = offset / PFS_BLOCK_SIZE;
//     int32_t end_block = (offset + num_bytes - 1) / PFS_BLOCK_SIZE;
//     size_t bytes_read = 0;
//     char* dest_buf = static_cast<char*>(buf);

//     for (int32_t block_num = start_block; block_num <= end_block; block_num++) {
//         auto& file_blocks = cache[filename];
//         auto block_it = file_blocks.find(block_num);

//         if (block_it == file_blocks.end()) {
//             return -1; // Block not in cache
//         }

//         CacheBlock& block = block_it->second;
//         block.last_access = std::time(nullptr);

//         // Calculate offsets within this block
//         size_t block_start = (block_num == start_block) ? offset % PFS_BLOCK_SIZE : 0;
//         size_t block_end = (block_num == end_block) ?
//                           ((offset + num_bytes - 1) % PFS_BLOCK_SIZE) + 1 :
//                           PFS_BLOCK_SIZE;
//         size_t bytes_to_read = block_end - block_start;

//         // Copy data from cache to destination buffer
//         std::memcpy(dest_buf + bytes_read,
//                    block.data.data() + block_start,
//                    bytes_to_read);
//         bytes_read += bytes_to_read;
//     }

//     return bytes_read;
// }

int PFSCache::readFromCache(const std::string &filename, int32_t block_num,
                            void *buf, size_t block_offset, size_t bytes_to_read)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Find the block in cache
    auto &file_blocks = cache[filename];
    auto block_it = file_blocks.find(block_num);

    if (block_it == file_blocks.end())
    {
        return -1; // Block not in cache
    }

    CacheBlock &block = block_it->second;

    // Validate offset and length
    if (block_offset >= block.data.size())
    {
        return -1; // Invalid offset
    }

    // Adjust bytes_to_read if it would exceed block size
    bytes_to_read = std::min(bytes_to_read, block.data.size() - block_offset);

    // Copy requested portion from cache to destination buffer
    std::memcpy(buf, block.data.data() + block_offset, bytes_to_read);

    // Update access time
    block.last_access = std::time(nullptr);

    return bytes_to_read;
}

int PFSCache::writeToCache(const std::string &filename, int32_t block_num, const void *buf,
                           off_t offset, size_t num_bytes)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    auto &block = cache[filename][block_num];

    // If block doesn't exist in cache or is wrong size, initialize it
    if (block.data.size() != PFS_BLOCK_SIZE)
    {
        block.data.resize(PFS_BLOCK_SIZE);
        block.filename = filename;
        block.block_number = block_num;
    }

    // Calculate offset within the block
    size_t block_offset = offset % PFS_BLOCK_SIZE;
    size_t bytes_to_write = std::min(num_bytes,
                                     PFS_BLOCK_SIZE - block_offset);

    // Copy data to cache
    std::memcpy(block.data.data() + block_offset, buf, bytes_to_write);
    block.is_dirty = true;
    block.last_access = std::time(nullptr);

    return bytes_to_write;
}

void PFSCache::addToCache(const std::string &filename, int32_t block_num,
                          const void *data, size_t size)
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Check if we need to evict a block
    if (getCacheSize() >= max_blocks)
    {
        evictBlock();
    }

    auto &block = cache[filename][block_num];
    block.data.resize(size);
    std::memcpy(block.data.data(), data, size);
    block.filename = filename;
    block.block_number = block_num;
    block.is_dirty = false;
    block.last_access = std::time(nullptr);
}

void PFSCache::removeFromCache(const std::string &filename, int32_t block_num)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto file_it = cache.find(filename);
    if (file_it != cache.end())
    {
        file_it->second.erase(block_num);
        if (file_it->second.empty())
        {
            cache.erase(file_it);
        }
    }
}

bool PFSCache::isDirty(const std::string &filename, int32_t block_num)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto file_it = cache.find(filename);
    if (file_it != cache.end())
    {
        auto block_it = file_it->second.find(block_num);
        if (block_it != file_it->second.end())
        {
            return block_it->second.is_dirty;
        }
    }
    return false;
}

void PFSCache::markDirty(const std::string &filename, int32_t block_num)
{
    std::cout << "markDirty called with filename: " << filename
              << ", block_num: " << block_num << std::endl;

    std::lock_guard<std::mutex> lock(cache_mutex);

    // Check if filename exists in the cache
    if (cache.find(filename) == cache.end())
    {
        std::cerr << "Filename " << filename << " not found in cache!" << std::endl;
        return; // Handle gracefully or throw an exception
    }

    // Check if block_num exists for the filename
    if (cache[filename].find(block_num) == cache[filename].end())
    {
        std::cerr << "Block " << block_num << " not found for filename: " << filename << std::endl;
        return; // Handle gracefully or throw an exception
    }

    cache[filename][block_num].is_dirty = true;
    std::cout << "Block " << block_num << " marked as dirty for filename: " << filename << std::endl;
}


bool PFSCache::getBlock(const std::string &filename, int32_t block_num,
                        void **data, size_t *size)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    auto file_it = cache.find(filename);
    if (file_it != cache.end())
    {
        auto block_it = file_it->second.find(block_num);
        if (block_it != file_it->second.end())
        {
            *data = block_it->second.data.data();
            *size = block_it->second.data.size();
            return true;
        }
    }
    return false;
}

size_t PFSCache::getCacheSize()
{
    size_t total = 0;
    for (const auto &file : cache)
    {
        total += file.second.size();
    }
    return total;
}

void PFSCache::evictBlock()
{
    // Find least recently used block
    std::string evict_file;
    int32_t evict_block = -1;
    std::time_t oldest_access = std::time(nullptr);

    for (const auto &file : cache)
    {
        for (const auto &block : file.second)
        {
            if (block.second.last_access < oldest_access && !block.second.is_dirty)
            {
                oldest_access = block.second.last_access;
                evict_file = file.first;
                evict_block = block.first;
            }
        }
    }

    // If found a non-dirty block, evict it
    if (evict_block != -1)
    {
        removeFromCache(evict_file, evict_block);
        return;
    }

    // If all blocks are dirty, write back the oldest one
    oldest_access = std::time(nullptr);
    for (const auto &file : cache)
    {
        for (const auto &block : file.second)
        {
            if (block.second.last_access < oldest_access)
            {
                oldest_access = block.second.last_access;
                evict_file = file.first;
                evict_block = block.first;
            }
        }
    }

    if (evict_block != -1)
    {
        // Write back dirty block
        auto &block = cache[evict_file][evict_block];
        writeBackBlock(evict_file, evict_block, block.data.data(), block.data.size());
        removeFromCache(evict_file, evict_block);
    }
}

void PFSCache::writeBackBlock(const std::string &filename, int32_t block_num,
                              const void *data, size_t size)
{
    // This would call back to your file server write implementation
    // You'll need to properly integrate this with your existing write functionality
    // Similar to your pfs_doTheWrite function
}
