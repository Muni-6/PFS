#include "pfs_cache.hpp"
#include "pfs_api.hpp"
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
extern int writeBlockToServer(const char *filename, int block_num, const void *block_data);
PFSCache::PFSCache(size_t max_cache_blocks,struct pfs_execstat& global_cache_stat) : max_blocks(max_cache_blocks), global_cache_stat(global_cache_stat) {}

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

void PFSCache::WriteBackAllAndClear()
{
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Iterate through all files
    for (auto &file_entry : cache)
    {
        const std::string &filename = file_entry.first;
        auto &blocks = file_entry.second;

        // Iterate through all blocks of the file
        for (auto &block_entry : blocks)
        {
            int32_t block_num = block_entry.first;
            CacheBlock &block = block_entry.second;

            // If block is dirty, write it back
            if (block.is_dirty)
            {
                // Attempt to write the block back to the server
                writeBackBlock(filename, block_num, block.data.data(), block.data.size());
                // After successful writeback, consider resetting dirty flag if needed
                // But since we're clearing cache anyway, this isn't strictly necessary
            }
        }
    }

    // Clear all cached data
    cache.clear();

    // If you maintain any LRU or other metadata structures, clear them here
    // For instance, if you have something like: lru_list.clear();

    std::cout << "All dirty blocks have been written back and the cache is now cleared." << std::endl;
}

void PFSCache::WriteBackAllFileName(std::string& file_name)
{
    std::lock_guard<std::mutex> lock(cache_mutex);
    std::cout << "[DEBUG] WriteBackAllFileName called for file: " << file_name << std::endl;

    bool file_found = false;

    // Iterate through all files in the cache
    for (auto &file_entry : cache)
    {
        const std::string &filename = file_entry.first;

        if (file_name == filename)
        {
            file_found = true;
            std::cout << "[DEBUG] Found matching file in cache: " << filename << std::endl;

            auto &blocks = file_entry.second;

            // Iterate through all blocks of the file
            for (auto &block_entry : blocks)
            {
                int32_t block_num = block_entry.first;
                CacheBlock &block = block_entry.second;

                // Check if the block is dirty
                if (block.is_dirty)
                {
                    std::cout << "[DEBUG] Dirty block found. Writing back block number: " << block_num
                              << " for file: " << filename << std::endl;

                    // Attempt to write the block back to the server
                    global_cache_stat.num_close_writebacks++;
                    writeBackBlock(filename, block_num, block.data.data(), block.data.size());

                    global_cache_stat.num_writebacks--;

                    std::cout << "[DEBUG] Successfully wrote back block number: " << block_num
                              << " for file: " << filename << std::endl;
                }
                else
                {
                    std::cout << "[DEBUG] Block number: " << block_num
                              << " for file: " << filename << " is not dirty. Skipping write back." << std::endl;
                }
            }

            // Stop searching after processing the specified file
            break;
        }
        else
        {
            std::cout << "[DEBUG] Skipping file: " << filename << " as it does not match " << file_name << std::endl;
        }
    }

    if (!file_found)
    {
        std::cerr << "[ERROR] File not found in cache: " << file_name << std::endl;
        return;
    }

    // Clear all cached data
    cache.clear();
    global_cache_stat.num_close_evictions++;
    std::cout << "[DEBUG] Cache cleared after writing back all dirty blocks for file: " << file_name << std::endl;

    // Clear any associated metadata if applicable
    // Example: lru_list.clear();
    // std::cout << "[DEBUG] 
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

    std::cout << "Attempting to add block " << block_num
              << " of file \"" << filename << "\" to cache." << std::endl;

    // Check if we need to evict a block
    if (getCacheSize() >= max_blocks)
    {
        std::cout << "Cache is full. Current size: " << getCacheSize()
                  << ", Max allowed: " << max_blocks << ". Evicting a block." << std::endl;
        evictBlock();
    }

    auto &block = cache[filename][block_num];
    block.data.resize(size);

    std::cout << "Resized block data to " << size << " bytes for block " << block_num << "." << std::endl;

    std::memcpy(block.data.data(), data, size);
    std::cout << "Copied " << size << " bytes of data into cache for block " << block_num << "." << std::endl;

    block.filename = filename;
    block.block_number = block_num;
    block.is_dirty = false;
    block.last_access = std::time(nullptr);

    std::cout << "Block " << block_num << " of file \"" << filename
              << "\" added to cache. Last access time set to "
              << block.last_access << "." << std::endl;
}

void PFSCache::removeFromCache(const std::string &filename, int32_t block_num)
{
    // std::lock_guard<std::mutex> lock(cache_mutex);
    auto file_it = cache.find(filename);
    if (file_it != cache.end())
    {
        file_it->second.erase(block_num);
        if (file_it->second.empty())
        {
            global_cache_stat.num_invalidations++;
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

    std::cout << "Evicting block...\n";

    for (const auto &file : cache)
    {
        for (const auto &block : file.second)
        {
            if (block.second.last_access <= oldest_access && !block.second.is_dirty)
            {
                oldest_access = block.second.last_access;
                evict_file = file.first;
                evict_block = block.first;

                global_cache_stat.num_evictions++;

                std::cout << "Found non-dirty block candidate for eviction: "
                            << "file=" << evict_file << ", block=" << evict_block
                            << ", last_access=" << oldest_access << "\n";
            }
        }
    }

    // If found a non-dirty block, evict it
    if (evict_block != -1)
    {
        std::cout << "Evicting non-dirty block: file=" << evict_file << ", block=" << evict_block << "\n";
        removeFromCache(evict_file, evict_block);
        return;
    }

    std::cout << "No non-dirty blocks found. Looking for oldest dirty block...\n";

    // If all blocks are dirty, write back the oldest one
    // Initialize oldest_access with the current time
oldest_access = std::time(nullptr);
std::cout << "Initializing oldest_access: " << oldest_access << "\n";

// Iterate over cache files and blocks
for (const auto &file : cache)
{
    std::cout << "Checking file: " << file.first << "\n";
    for (const auto &block : file.second)
    {
        std::cout << "Checking block: " << block.first << "\n";
        if (block.second.last_access <= oldest_access)
        {
            oldest_access = block.second.last_access;
            evict_file = file.first;
            evict_block = block.first;

            std::cout << "Found dirty block candidate for write-back: "
                        << "file=" << evict_file << ", block=" << evict_block
                        << ", last_access=" << oldest_access << "\n";
        }
        else
        {
            std::cout << "Block not older than current oldest: "
                        << "file=" << file.first << ", block=" << block.first
                        << ", last_access=" << block.second.last_access << "\n";
        }
    }
}

    if (evict_block != -1)
    {
        std::cout << "Writing back dirty block: file=" << evict_file << ", block=" << evict_block << "\n";
        auto &block = cache[evict_file][evict_block];
        writeBackBlock(evict_file, evict_block, block.data.data(), block.data.size());
        std::cout << "Removing block from cache: file=" << evict_file << ", block=" << evict_block << "\n";
        removeFromCache(evict_file, evict_block);
    }
    else
    {
        std::cout << "No blocks found for eviction or write-back.\n";
    }
}
void PFSCache::writeBackBlock(const std::string &filename, int32_t block_num,
                              const void *data, size_t size)
{
    std::cout << "writeBackBlock called for filename: " << filename
              << ", block_num: " << block_num << std::endl;
              global_cache_stat.num_writebacks++;
    // std::cout << "Press Enter to continue...\n";
    // sleep(20);
    int result = writeBlockToServer(filename.c_str(), block_num, data);
    if (result < 0)
    {
        std::cerr << "Failed to write back block " << block_num
                  << " for file " << filename << std::endl;
    }
    else
    {
        std::cout << "Successfully wrote back block " << block_num
                  << " for file " << filename << std::endl;
    }
    // This would call back to your file server write implementation
    // You'll need to properly integrate this with your existing write functionality
    // Similar to your pfs_doTheWrite function
}
