#include "pfs_api.hpp"
#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "pfs_proto/pfs_fileserver.grpc.pb.h"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using pfs::FileServer;
using pfs::MetaServer;
std::unique_ptr<MetaServer::Stub> metaserver_stub;
std::vector<std::unique_ptr<FileServer::Stub>> fileserver_stubs;
struct BlockCacheInfo
{
    bool is_cacheable;
    bool is_valid;
    int32_t block_number;
    std::string filename;
};
std::int32_t client_id;
std::unique_ptr<PFSCache> client_cache;
// Fileserver Data Structures
std::unordered_map<std::string, int> fileStripeWidths;
std::unordered_map<std::string, pfs_metadata> metadataMap;
std::unordered_map<int, std::string> fd_to_filename;
std::unordered_map<std::string, int> filename_to_fd;
std::unordered_map<int, int> file_mode;
std::unordered_map<std::string, std::unordered_map<int32_t, BlockCacheInfo>> block_cache_status;
// Can make the call as writeBlockToServer(const char *filename, int block_num, const void *block_data)
int readBlockFromServer(const char *filename, int block_num, void *read_buf)
{
    // if (fileStripeWidths.find(filename) == fileStripeWidths.end()) {
    //     std::cerr << "Error: Filename " << filename << " not found in stripe widths map." << std::endl;
    //     return -1;
    // }

    int stripe_width = metadataMap[filename].recipe.stripe_width;
    if (stripe_width <= 0) {
        std::cerr << "Error: Invalid stripe width for file " << filename << ": " << stripe_width << std::endl;
        return -1;
    }
    // Generate block name based on file and block number
    // int stripe_width = fileStripeWidths[filename];
    int block_group_index = block_num / STRIPE_BLOCKS;
    int server_index = block_group_index % stripe_width;
    std::cout << "The server index for the block " << block_num << " is : " << server_index << std::endl;
    std::string block_name = std::string(filename) + "." + std::to_string(server_index);
    std::cout << "The file server name for the block " << block_num << " is : " << block_name << std::endl;

    int block_index_server = ((block_num / (stripe_width * STRIPE_BLOCKS)) * STRIPE_BLOCKS) + (block_num % STRIPE_BLOCKS);
    int block_offset = block_index_server * PFS_BLOCK_SIZE;

    std::cout << "The block offset for the block " << block_num << " is : " << block_offset << std::endl;

    // Prepare the gRPC request
    pfs::ReadChunkRequest request;
    request.set_filename(block_name);
    request.set_offset(block_offset); // Block-level offset
    request.set_num_bytes(PFS_BLOCK_SIZE);

    // Perform the gRPC call
    pfs::ReadChunkResponse response;
    grpc::ClientContext context;

    // Assuming fileserver_stubs maps block_num to the appropriate fileserver
    // int server_index = block_num % fileserver_stubs.size();
    Status status = fileserver_stubs[server_index]->ReadChunk(&context, request, &response);

    // Handle errors
    if (!status.ok() || !response.success())
    {
        std::cerr << "Error reading block " << block_num
                  << " from server " << server_index
                  << ": " << status.error_message() << std::endl;
        return -1;
    }

    // Copy the data to the provided buffer
    std::string chunk_data = response.data();
    memcpy(read_buf, chunk_data.data(), chunk_data.size());

    std::cout << "Successfully read " << chunk_data.size()
              << " bytes from block " << block_num
              << " on server " << server_index << std::endl;

    return chunk_data.size(); // Return the number of bytes read
}
int writeBlockToServer(const char *filename, int block_num, const void *block_data)
{
    // Determine the server index based on block number
    int stripe_width = fileStripeWidths[filename];
    int block_group_index = block_num / STRIPE_BLOCKS;
    int server_index = block_group_index % stripe_width;
    std::cout << "The server index for the block " << block_num << " is : " << server_index << std::endl;
    std::string block_name = std::string(filename) + "." + std::to_string(server_index);
    std::cout << "The file server name for the block " << block_num << " is : " << block_name << std::endl;

    int block_index_server = ((block_num / (stripe_width * STRIPE_BLOCKS)) * STRIPE_BLOCKS) + (block_num % STRIPE_BLOCKS);
    int block_offset = block_index_server * PFS_BLOCK_SIZE;

    std::cout << "The block offset for the block " << block_num << " is : " << block_offset << std::endl;

    // Prepare the gRPC WriteChunkRequest
    pfs::WriteChunkRequest request;
    request.set_filename(block_name);                                                     // Set the block name
    request.set_offset(block_offset);                                                     // Set the block-level offset
    request.set_data(std::string(static_cast<const char *>(block_data), PFS_BLOCK_SIZE)); // Add the block data

    // Perform the gRPC call
    pfs::WriteChunkResponse response;
    grpc::ClientContext context;

    // Send the request to the appropriate server
    Status status = fileserver_stubs[server_index]->WriteChunk(&context, request, &response);

    // Handle errors
    if (!status.ok() || !response.success())
    {
        std::cerr << "Error writing block " << block_num
                  << " to server " << server_index
                  << ": " << status.error_message() << std::endl;
        return -1;
    }

    std::cout << "Successfully wrote " << response.bytes_written()
              << " bytes to block " << block_num
              << " on server " << server_index << std::endl;

    return response.bytes_written(); // Return the number of bytes written
}
int fetchAndCacheBlock(const std::string &filename, int32_t block_num,
                       void *buf, size_t offset, size_t num_bytes)
{
     std::cout << "fetchAndCacheBlock called with filename: " << filename
              << ", block_num: " << block_num
              << ", offset: " << offset
              << ", num_bytes: " << num_bytes << std::endl;
    // Allocate buffer for entire block
    std::vector<char> block_buffer(PFS_BLOCK_SIZE);

    // Read entire block from server
    std::cout << "Allocating buffer of size: " << PFS_BLOCK_SIZE << std::endl;
    int result = readBlockFromServer(filename.c_str(), block_num, block_buffer.data());
   if (result < 0)
    {
        std::fill_n(static_cast<char *>(buf), PFS_BLOCK_SIZE, 0xFF);
    }

    std::cout << "Block read successfully, result: " << result << std::endl;
    // Add to cache
    client_cache->addToCache(filename, block_num, block_buffer.data(), result);
    block_cache_status[filename][block_num].is_valid = true;

    std::cout << "Added block to cache for filename: " << filename << ", block_num: " << block_num << std::endl;

    // Copy requested portion to user buffer
    // if itd read req we copy the portion to user buffer
    memcpy(buf, block_buffer.data() + offset, num_bytes);
    // else if its write we should write to that portion of cache
    // doit
    std::cout << "Copied " << num_bytes << " bytes to user buffer" << std::endl;

    return num_bytes;
}
class ClientTokenManager
{
private:
    std::int32_t client_id;
    std::unique_ptr<grpc::ClientContext> context_ptr;
    std::thread reader_thread;
    std::mutex tokens_mutex;

    struct TokenRange {
        off_t start_offset;
        off_t end_offset;
        bool is_write;
        std::string filename;
        std::int32_t fd;
        std::string status; // "active" or "completed"
        std::mutex status_mutex;
        std::condition_variable status_cv;

        TokenRange(off_t start, off_t end, bool write, const std::string& file, int file_descriptor)
            : start_offset(start),
              end_offset(end),
              is_write(write),
              filename(file),
              fd(file_descriptor),
              status("active") {}
    };

    std::list<TokenRange> held_tokens;
    // std::vector<std::unique_ptr<TokenRange>> held_tokens;

    std::shared_ptr<grpc::ClientReaderWriter<pfs::StreamRequest, pfs::StreamResponse>> stream;
    PFSCache &client_cache; // Reference to the global cache

public:
    bool CompleteTokenRange(const std::string &filename, off_t start, off_t end)
    {
        std::lock_guard<std::mutex> lock(tokens_mutex);
         std::cout << "CompleteTokenRange called for filename: " << filename
              << ", start: " << start << ", end: " << end << std::endl;

        for (auto &token : held_tokens)
        {
            if (token.filename == filename &&
                token.start_offset <= start &&
                token.end_offset >= end)
            {
                std::lock_guard<std::mutex> status_lock(token.status_mutex);
                token.status = "completed";
                std::cout << "Token range completed for " << filename << " from " << start << " to " << end << std::endl;
                token.status_cv.notify_all();
                return true;
            }
        }
         std::cerr << "No token found for completion in range for " << filename << " from " << start << " to " << end << std::endl;
        return false;
    }

    bool WaitForConflictingTokensCompletion(const std::string &filename, off_t start, off_t end)
    {
       // std::unique_lock<std::mutex> lock(tokens_mutex);

         std::cout << "WaitForConflictingTokensCompletion called for filename: " << filename
              << ", start: " << start << ", end: " << end << std::endl;
        // Find all conflicting tokens
        std::vector<TokenRange *> conflicting_tokens;
        for (auto &token : held_tokens)
        {
            if (token.filename == filename &&
                !((start > token.end_offset) || (end < token.start_offset)))
            {
                conflicting_tokens.push_back(&token);
            }
        }

        if (conflicting_tokens.empty())
        {
             std::cout << "No conflicting tokens found for range " << start << " to " << end << std::endl;
            return true;
        }

        // Wait for all conflicting tokens to complete
        for (auto *token : conflicting_tokens)
        {
            std::unique_lock<std::mutex> status_lock(token->status_mutex);
            token->status_cv.wait(status_lock, [token]()
                                  { return token->status == "completed"; });
                                  std::cout << "Conflicting token completed for " << token->filename << " from " << token->start_offset << " to " << token->end_offset << std::endl;
        }

        return true;
    }

    ClientTokenManager(std::int32_t id, std::shared_ptr<MetaServer::Stub> stub, PFSCache &cache) : client_id(id), client_cache(cache)
    {
        context_ptr = std::make_unique<grpc::ClientContext>();
        stream = stub->ClientStream(context_ptr.get());
        pfs::StreamRequest initial_request;
        initial_request.set_client_id(client_id);
        if (!stream->Write(initial_request))
        {
            std::cerr << "Failed to send initial stream request" << std::endl;
            return;
        }
        reader_thread = std::thread([this]()
                                    {
            pfs::StreamResponse response;
            while (stream->Read(&response)) {
                std::cout<<"Got response from the server to the client with the action "<<response.action()<<std::endl;
                std::lock_guard<std::mutex> lock(tokens_mutex);
                
                if (response.action() == "invalidate") {
                     std::cout << "Client should revoke the token from : " << response.start_offset() <<" to "<<response.end_offset()<< std::endl<< std::endl;
                     if(!response.invalidate()){
                        RemoveTokenRange(response.filename(), 
                                   response.start_offset(), 
                                   response.end_offset());  
                     }
                      std::cout << "Client revoked the token from : " << response.start_offset()<<" to "<<response.end_offset()<<" and now sending the ack to the server."<< std::endl;
                      for (const auto &block_num : response.invalidate_blocks())
                      {
                          std::cout << "Processing block " << block_num << " for file: " << response.filename() << std::endl;

                          auto &block_info = block_cache_status[response.filename()][block_num];

                          if (block_info.is_valid)
                          {
                              std::cout << "Block " << block_num << " is valid in cache. Marking as dirty and writing back." << std::endl;
                              client_cache.markDirty(response.filename(), block_num);
                              writeBackBlock(response.filename(), block_num);
                          }
                          else
                          {
                              std::cout << "Block " << block_num << " is not valid in cache. Skipping write-back." << std::endl;
                          }

                          block_info.is_cacheable = false;
                          block_info.is_valid = false;

                          std::cout << "Removing block " << block_num << " from cache." << std::endl;
                          client_cache.removeFromCache(response.filename(), block_num);
                      }

                    pfs::StreamRequest invalidate_ack;
                    invalidate_ack.set_action("invalidate_ack");
                    invalidate_ack.set_client_id(this->client_id);
                    invalidate_ack.set_file_descriptor(response.file_descriptor());
                    invalidate_ack.set_start_offset(response.start_offset());
                    invalidate_ack.set_end_offset(response.end_offset());
                    invalidate_ack.set_request_id(response.request_id());
                     if (!stream->Write(invalidate_ack)) {
                       std::cerr << "Failed to send acknowledgment to server." << std::endl;
                    } else {
                        std::cout << "Sent invalidate_ack to server." << std::endl;
                    }      
                }
                else if (response.action() == "grant") {
                    // Add new token range
                    AddTokenRange(response.filename(),
                                response.start_offset(),
                                response.end_offset(),
                                response.is_write(),
                                response.file_descriptor());
                     pfs::StreamRequest grant_ack;
                    grant_ack.set_action("grant_ack");
                    grant_ack.set_client_id(this->client_id);
                    grant_ack.set_file_descriptor(response.file_descriptor());
                    grant_ack.set_start_offset(response.start_offset());
                    grant_ack.set_end_offset(response.end_offset());
                    grant_ack.set_request_id(response.request_id());
                     if (!stream->Write(grant_ack)) {
                       std::cerr << "Failed to send acknowledgment to server." << std::endl;
                    } else {
                        std::cout << "Sent grant_ack to server." << std::endl;
                    }                  

                }
            }  std::cout << "Reader thread ending" << std::endl; });
        reader_thread.detach();
    }

    void writeBackBlock(const std::string &filename, int32_t block_num)
    {
         std::cout << "writeBackBlock called for filename: " << filename
              << ", block_num: " << block_num << std::endl;

        // Get block data from cache
        void *block_data;
        size_t block_size;
        if (client_cache.getBlock(filename, block_num, &block_data, &block_size))
        {
             std::cout << "Block data retrieved from cache for " << filename << ", block_num: " << block_num << std::endl;
            // Write back to server
            writeBlockToServer(filename.c_str(), block_num, block_data);
            std::cout << "Block data written back to server for " << filename << ", block_num: " << block_num << std::endl;
        }
        else
        {
            std::cerr << "Block not found in cache for " << filename << ", block_num: " << block_num << std::endl;
        }
    }
    bool HasTokenForRange(const std::int32_t &fd, off_t start, off_t end, bool write_access)
    {
         std::cout << "HasTokenForRange called for fd: " << fd
              << ", start: " << start << ", end: " << end
              << ", write_access: " << write_access << std::endl;
        std::lock_guard<std::mutex> lock(tokens_mutex);
        // std::cout << "Inside HasTokenForRange " << std::endl;
        if (held_tokens.size() == 0)
        {
            // std::cout << "held_tokens size is zero " << std::endl;
             std::cout << "held_tokens size is zero, client isn't holding any token" << std::endl;
            return false;
        }
        for (const auto &token : held_tokens)
        {
            if (token.fd == fd &&
                !((start > token.end_offset) || (end < token.start_offset)) &&
                token.status == "active" &&
                (write_access || token.is_write))
            {
                // Need to wait for conflicting token completion
                 std::cout << "Conflicting token found, waiting for token to complete" << std::endl;
                return false;
            }
        }

        for (const auto &token : held_tokens)
        {
            if (token.fd == fd &&
                token.start_offset <= start &&
                token.end_offset >= end &&
                token.status == "active" &&
                (write_access == token.is_write))
            {
                 std::cout << "Token found for the requested range, returning true" << std::endl;
                return true;
            }
        }
         std::cout << "Client isn't holding the token, requesting for the token now" << std::endl;
        return false;
    }

private:
    void RemoveTokenRange(const std::string &filename, off_t start, off_t end)
    {
         std::cout << "RemoveTokenRange called for filename: " << filename
              << ", start: " << start << ", end: " << end << std::endl;
        if (!WaitForConflictingTokensCompletion(filename, start, end))
        {
            std::cerr << "Failed waiting for conflicting tokens" << std::endl;
            // return -1;
        }
        auto it = held_tokens.begin();
        std::list<TokenRange> new_tokens;
        while (it != held_tokens.end())
        {
            std::cout << "Checking token range for filename: " << it->filename
                  << ", start_offset: " << it->start_offset
                  << ", end_offset: " << it->end_offset << std::endl;
            if (it->filename == filename &&
                ((start <= it->start_offset && end >= it->start_offset) || // Overlap with start
                 (start <= it->end_offset && end >= it->end_offset)))      // Overlap with end
            {
                // Check if there is a left portion that is not conflicting
                if (start > it->start_offset)
                {
                    // Add the left non-conflicting portion
                    std::cout << "Shrink of the token happened on the left side on the CLIENT SIDE " << it->start_offset << " to " << start - 1 << " with the token " << it->is_write << std::endl;
                    new_tokens.emplace_back(it->start_offset, start - 1, it->is_write, it->filename, it->fd);
                }

                // Check if there is a right portion that is not conflicting
                if (end < it->end_offset)
                {
                    // Add the right non-conflicting portion
                    std::cout << "Shrink of the token happened on the right side on the CLIENT SIDE " << end + 1 << " to " << it->end_offset << " with the token " << it->is_write << std::endl;
                    new_tokens.emplace_back(end + 1, it->end_offset, it->is_write, it->filename, it->fd);
                }

                // Erase the original conflicting token
                it = held_tokens.erase(it);
            }
            else
            {
                ++it;
            }
            // ++it;
        }
        // held_tokens.insert(held_tokens.end(), new_tokens.begin(), new_tokens.end());
        for (auto &token : new_tokens)
        {
            held_tokens.emplace_back(
                token.start_offset,
                token.end_offset,
                token.is_write,
                token.filename,
                token.fd);
        }
        std::cout << "Token range removal completed for " << filename << " from " << start << " to " << end << std::endl;
    }
    void AddTokenRange(const std::string &filename, off_t start, off_t end, bool is_write, std::int32_t fd)
    {
        std::cout << "AddTokenRange called for filename: " << filename
              << ", start: " << start << ", end: " << end
              << ", is_write: " << is_write << ", fd: " << fd << std::endl;
        held_tokens.emplace_back(start, end, is_write, filename, fd);

         std::cout << "Token added to held_tokens for filename: " << filename
              << ", start: " << start << ", end: " << end
              << ", is_write: " << is_write << std::endl;

    }
};

std::unique_ptr<ClientTokenManager> token_manager;
bool CheckServerAlive(std::int32_t id)
{
    pfs::AliveRequest request;
    pfs::AliveResponse response;
    ClientContext context;
    Status status = fileserver_stubs[id]->CheckAliveServer(&context, request, &response);

    return status.ok() && response.alive();
}

bool RequestToken(const std::int64_t &fd, off_t start, off_t end, bool write_access)
{
    grpc::ClientContext context;
    pfs::TokenRequest request;
    pfs::TokenResponse response;
    sleep(5);
    request.set_client_id(client_id);
    request.set_file_descriptor(fd);
    request.set_start_offset(start);
    request.set_end_offset(end);
    request.set_is_write(write_access);

    Status status = metaserver_stub->RequestToken(&context, request, &response);

    if (status.ok() && response.success())
    {
        // Get the filename for this fd
        std::string filename = response.filename();

        // Update block cache status based on response
        for (const auto &block_num : response.cacheable_blocks())
        {
            block_cache_status[filename][block_num].is_cacheable = true;
            block_cache_status[filename][block_num].block_number = block_num;
            block_cache_status[filename][block_num].filename = filename;
            // Note: is_valid remains false until we actually cache the block
        }

        return true;
    }
    return false;
}

bool CheckServerAliveMeta()
{
    pfs::AliveRequestMeta request;
    pfs::AliveResponseMeta response;
    ClientContext context;
    Status status = metaserver_stub->CheckAliveMetaServer(&context, request, &response);

    return status.ok() && response.alive();
}

std::int32_t RegisterClient()
{
    pfs::RegisterClientRequest request;
    // std::cout << "Get my hostname -----------------";
    request.set_hostname("My host is Muni");

    pfs::RegisterClientResponse response;
    grpc::ClientContext context;

    Status status = metaserver_stub->RegisterClient(&context, request, &response);
    if (!status.ok() || !response.success())
    {
        std::cerr << "Failed to register client" << std::endl;
        return -1;
    }
    // std::cout << "Got my hostname -----------------" << response.client_id() << std::endl;
    return response.client_id();
}
int pfs_initialize()
{
    // Read pfs_list.txt
    std::ifstream file("../pfs_list.txt"); // Replace "filename.txt" with the path to your file
    if (!file.is_open())
    {
        std::cerr << "Error opening file." << std::endl;
        return 1;
    }

    std::string line;
    getline(file, line);
    // std::cout << "MetaServer Address is -----------------------------------------" << line << std::endl;
    metaserver_stub = MetaServer::NewStub(
        grpc::CreateChannel(line, grpc::InsecureChannelCredentials()));
    auto token_manager_stub = MetaServer::NewStub(
        grpc::CreateChannel(line, grpc::InsecureChannelCredentials()));

    if (!CheckServerAliveMeta())
    {
        std::cerr << "Metadata server is not online." << std::endl;
        return -1;
    }

    int server_id = 0;
    while (std::getline(file, line))
    {
        // Create gRPC channel for file server
        fileserver_stubs.push_back(FileServer::NewStub(
            grpc::CreateChannel(line, grpc::InsecureChannelCredentials())));
        // std::cout << "FileServer Address is -----------------------------------------" << line << std::endl;
        // Check if file server is online
        if (!CheckServerAlive(server_id))
        {
            std::cerr << "File server " << server_id << " is not online." << std::endl;
            return -1;
        }
        server_id++;
    }
    file.close();
    // Check if all servers (NUM_FILE_SERVERS + 1) are online
    // Connect with metaserver using gRPC
    metaserver_api_temp();

    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    for (int i = 0; i < NUM_FILE_SERVERS; ++i)
    {
        fileserver_api_temp();
    }
    // std::cout << "Register Client is called-------------------?--------- " << std::endl;
    client_id = RegisterClient();
    // should we create a bidirectional stream while initialization only??

// Step 1: Initialize the cache first
client_cache = std::make_unique<PFSCache>(CLIENT_CACHE_BLOCKS);

// Step 2: Use the existing cache instance to initialize the token manager
token_manager = std::make_unique<ClientTokenManager>(client_id, std::move(token_manager_stub), *client_cache);

    return client_id;
}

int pfs_finish(int client_id)
{

    return 0;
}

int pfs_create(const char *filename, int stripe_width)
{
    std::cout << "Inside create file " << std::endl;
    // Have to check for duplicate filename, if metaserver stub is null, if stripe width is greater than the no of file servers

    // Have to check for duplicate filename
    for (const auto &pair : fileStripeWidths)
    {
        if (pair.first == filename)
        {
            std::cerr << "Duplicate filename: " << filename << std::endl;
            return -1;
        }
    }

    if (!metaserver_stub)
    {
        std::cerr << "Metaserver stub is null" << std::endl;
        return -1;
    }

    if (stripe_width > fileserver_stubs.size())
    {
        std::cerr << "Invalid stripe width: " << stripe_width
                  << ". Number of available servers: " << fileserver_stubs.size() << std::endl;
        return -1;
    }

    pfs::CreateFileRequest meta_request;
    meta_request.set_filename(filename);
    meta_request.set_stripe_width(stripe_width);

    pfs::CreateFileResponse meta_response;
    grpc::ClientContext meta_context;

    Status meta_status = metaserver_stub->CreateFile(&meta_context, meta_request, &meta_response);
    if (!meta_status.ok() || !meta_response.success())
    {
        std::cerr << "Failed to create metadata on metaserver: " << meta_status.error_message() << std::endl;
        return -1;
    }
    std::cout << "Metadata for file " << filename << " created successfully." << std::endl;
    // int fd = meta_response.file_descriptor();
    // No fileserver handling as the there is no data to be written into the fileservers
    //  int fd = meta_response.file_descriptor();
    //  fileStripeWidths[fd] = stripe_width;
    //  fd_to_filename[fd] = filename;
    //  No file descriptor is assigned here
    fileStripeWidths[filename] = stripe_width;
    return 0;
}

int pfs_open(const char *filename, int mode)
{
    
    // Validate input
    if (filename == nullptr || strlen(filename) == 0)
    {
        std::cerr << "Invalid filename for pfs_open." << std::endl;
        return -1;
    }

    if (mode != 1 && mode != 2)
    {
        std::cerr << "Invalid mode for pfs_open. Expected 1 (read) or 2 (read/write)." << std::endl;
        return -1;
    }

    // Check if file is already open
    for (const auto &pair : fd_to_filename)
    {
        if (pair.second == filename)
        {
            std::cerr << "File is already open: " << filename << std::endl;
            return -1;
        }
    }

    // Step 1: Prepare the gRPC request to fetch file metadata
    pfs::OpenFileRequest request;
    request.set_filename(filename);
    request.set_mode(mode); // Mode can be read/write/etc

    pfs::OpenFileResponse response;
    grpc::ClientContext context;

    // Step 2: Communicate with the metadata server
    Status status = metaserver_stub->OpenFile(&context, request, &response);
    std::cout << "The status is ok : " << status.ok() << std::endl;
    std::cout << "The response success is : " << response.success() << std::endl;
    if (!status.ok() || !response.success())
    {
        std::cerr << "Failed to open file on metadata server: " << status.error_message() << std::endl;
        return -1;
    }

    // Step 3: Extract the file descriptor and other metadata
    int fd = response.file_descriptor();
        // Step 4: Update local structures for file descriptor and metadata management
    // fileStripeWidths[fd] = stripe_width;
    fd_to_filename[fd] = filename;
    filename_to_fd[filename] = fd;
    file_mode[fd] = mode;
    // int stripe_width = fileStripeWidths[fd];
    pfs_metadata metadata;
    int result = pfs_fstat(fd, &metadata);
    if (result == 0) {
        std::cout << "Metadata fetched successfully." << std::endl;
    } else {
        std::cerr << "Failed to fetch metadata." << std::endl;
    }
    metadataMap[filename] = metadata;


    std::cout << "File opened successfully with FD: " << fd << " in mode: " << mode << std::endl;

    return fd;
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset)
{
    // ...
    std::cout << "Inside pfs_read with fd: " << fd << ", num_bytes: " << num_bytes << ", offset: " << offset << std::endl;
    // Validate input parameters
    if (fd < 0 || buf == nullptr || num_bytes == 0)
    {
        std::cerr << "Invalid input parameters for pfs_read." << std::endl;
        return -1;
    }

    if (file_mode[fd] != 1)
    {
        std::cerr << "The mode is not 'read' for file descriptor " << fd << std::endl;
        return -1;
    }

    std::string filename = fd_to_filename[fd];
    std::cout << "Filename resolved for fd " << fd << ": " << filename << std::endl;

    // Calculate block range for this read
    int32_t start_block = offset / PFS_BLOCK_SIZE;
    int32_t end_block = (offset + num_bytes - 1) / PFS_BLOCK_SIZE;
    size_t bytes_read = 0;

    std::cout << "Start block: " << start_block << ", End block: " << end_block << std::endl;

    if (token_manager->HasTokenForRange(fd, offset, offset + num_bytes, 0))
    {
        // We have the read token - check cache status for each block
        std::cout << "Read token acquired for range " << offset << " to " << offset + num_bytes << std::endl;
        for (int32_t block_num = start_block; block_num <= end_block; block_num++)
        {

            auto &block_info = block_cache_status[filename][block_num];
            std::cout << "Checking block " << block_num << " for filename " << filename << std::endl;

            // Calculate current block's read boundaries
            size_t block_offset = (block_num == start_block) ? offset % PFS_BLOCK_SIZE : 0;
            size_t bytes_this_block = std::min(
                num_bytes - bytes_read,       // Remaining bytes to read
                PFS_BLOCK_SIZE - block_offset // Bytes till end of this block
            );

             std::cout << "Block " << block_num << ": block_offset = " << block_offset
                      << ", bytes_this_block = " << bytes_this_block << std::endl;

            if (block_info.is_cacheable && block_info.is_valid)
            {
                 std::cout << "Block " << block_num << " is cacheable and valid." << std::endl;
                // Block can be cached - but might not be in cache yet
                if (!client_cache->isBlockCached(filename, block_num))
                {
                    std::cout << "Block " << block_num << " not in cache, fetching..." << std::endl;
                    // Block not in cache - try to fetch it
                    int result = fetchAndCacheBlock(filename, block_num,
                                                    static_cast<char *>(buf) + bytes_read,
                                                    block_offset, bytes_this_block);
                    if (result < 0)
                    {
                        std::cerr << "Failed to fetch and cache block " << block_num << std::endl;
                        return -1;
                    }
                    bytes_read += result;
                      std::cout << "Fetched and cached block " << block_num << ", bytes_read: " << bytes_read << std::endl;
                }
                else
                {
                    std::cout << "Block " << block_num << " is in cache, reading from cache..." << std::endl;
                    int result = client_cache->readFromCache(filename, block_num,
                                                             static_cast<char *>(buf) + bytes_read,
                                                             block_offset, bytes_this_block);
                                                             
                     if (result < 0)
                    {
                        std::cerr << "Failed to read block " << block_num << " from cache" << std::endl;
                        return -1;
                    }
                    bytes_read += result;
                    // read from the cached block
                }
                // At this point, block is in cache
            }
            else if (!block_info.is_cacheable)
            {
                // If any block in range isn't cacheable, read everything directly
                std::cout << "Some blocks not cacheable, reading directly from servers" << std::endl;
                int result = readBlockFromServer(filename.c_str(), block_num, buf);
                 if (result < 0)
                {
                    std::cerr << "Failed to read block " << block_num << " from server" << std::endl;
                    return -1;
                }
                bytes_read += result; // reading from the server
            }
        }
        std::cout << "Finished reading " << bytes_read << " bytes with read token." << std::endl;
        return bytes_read;
    }
    else
    {
        // Don't have token - request it
        std::cout << "No read token available, requesting read token..." << std::endl;
        if (RequestToken(fd, offset, offset + num_bytes, 0))
        {
            std::cout << "Read token acquired after request." << std::endl;
            // Got the token - now some blocks might be cacheable
            for (int32_t block_num = start_block; block_num <= end_block; block_num++)
            {

                auto &block_info = block_cache_status[filename][block_num];
                std::cout << "Checking block " << block_num << " for filename " << filename << std::endl;

                // Calculate current block's read boundaries
                size_t block_offset = (block_num == start_block) ? offset % PFS_BLOCK_SIZE : 0;
                size_t bytes_this_block = std::min(
                    num_bytes - bytes_read,       // Remaining bytes to read
                    PFS_BLOCK_SIZE - block_offset // Bytes till end of this block
                );
                std::cout << "Block " << block_num << ": block_offset = " << block_offset
                          << ", bytes_this_block = " << bytes_this_block << std::endl;

                if (block_info.is_cacheable)
                {
                     std::cout << "Block " << block_num << " is cacheable." << std::endl;
                    // Block can be cached - but might not be in cache yet
                    if (!client_cache->isBlockCached(filename, block_num))
                    {
                         std::cout << "Block " << block_num << " not in cache, fetching..." << std::endl;
                        // Block not in cache - try to fetch it
                        int result = fetchAndCacheBlock(filename, block_num,
                                                        static_cast<char *>(buf) + bytes_read,
                                                        block_offset, bytes_this_block);
                        if (result < 0)
                        {
                            std::cerr << "Failed to fetch and cache block " << block_num << std::endl;
                            return -1;
                        }
                        bytes_read += result;
                    }
                    else
                    {
                        std::cout << "Block " << block_num << " is in cache, reading from cache..." << std::endl;
                        int result = client_cache->readFromCache(filename, block_num,
                                                                 static_cast<char *>(buf) + bytes_read,
                                                                 block_offset, bytes_this_block);
                        if (result < 0)
                        {
                            std::cerr << "Failed to read block " << block_num << " from cache" << std::endl;
                            return -1;
                        }
                        bytes_read += result;
                        // read from the cached block
                    }
                    // At this point, block is in cache
                }
                else if (!block_info.is_cacheable)
                {
                    // If any block in range isn't cacheable, read everything directly
                   std::cout << "Block " << block_num << " is not cacheable, reading directly from server..." << std::endl;
                    int result = readBlockFromServer(filename.c_str(), block_num, buf);
                    if (result < 0)
                    {
                        std::cerr << "Failed to read block " << block_num << " from server" << std::endl;
                        return -1;
                    }
                    bytes_read += result; // reading from the server
                }
            }
             std::cout << "Finished reading " << bytes_read << " bytes after requesting token." << std::endl;
            return bytes_read;
        }
        else
        {
            std::cerr << "Failed to get read token" << std::endl;
            return -1;
        }
    }
}

int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset)
{
    std::cout << "Inside pfs_write with fd: " << fd
              << ", num_bytes: " << num_bytes
              << ", offset: " << offset << std::endl;
    std::string filename = fd_to_filename[fd];

    std::cout << "Inside pfs_write " << std::endl;
    size_t bytes_written = 0;
    int pfs_write_result = 0;
    // TODO: Whether to put the edge case conditions here or in the pfs_doTheWrite
    if (fd < 0 || buf == nullptr || num_bytes == 0)
    {
        std::cerr << "Invalid input parameters for pfs_write." << std::endl;
        return -1;
    }

    if (file_mode[fd] != 2)
    {
        std::cerr << "The mode is not 'write' for file descriptor " << fd << std::endl;
        return -1;
    }

    // Check if file descriptor exists
    if (fd_to_filename.find(fd) == fd_to_filename.end())
    {
        std::cerr << "Filename not found for file descriptor: " << fd << std::endl;
        return -1;
    }
     std::cout << "Filename resolved for fd " << fd << ": " << filename << std::endl;
    if (!token_manager->WaitForConflictingTokensCompletion(filename, offset, offset + num_bytes))
    {
        std::cerr << "Failed waiting for conflicting tokens" << std::endl;
        return -1;
    }
    if (token_manager->HasTokenForRange(fd, offset, offset + num_bytes, 1))
    {
        // to do::change the running status of token
        std::cout << "Write token is already held by the client." << std::endl;
        int32_t start_block = offset / PFS_BLOCK_SIZE;
        int32_t end_block = (offset + num_bytes - 1) / PFS_BLOCK_SIZE;

        for (int block_num = start_block; block_num <= end_block; block_num++)
        {
             std::cout << "Processing block " << block_num << std::endl;
            auto &block_info = block_cache_status[filename][block_num];
            size_t block_offset = (block_num == start_block) ? offset % PFS_BLOCK_SIZE : 0;
            size_t bytes_this_block = std::min(
                num_bytes - bytes_written,    // Remaining bytes to write
                PFS_BLOCK_SIZE - block_offset // Bytes till end of this block
            );
            if (client_cache->isBlockCached(filename, block_num))
            {
                 std::cout << "Block " << block_num << " is cached. Writing to cache." << std::endl;
                int result = client_cache->writeToCache(
                    filename, block_num,
                    static_cast<const char *>(buf) + bytes_written,
                    block_offset, bytes_this_block);

                if (result < 0)
                {
                    std::cerr << "Error writing to cache for block " << block_num << std::endl;
                    return -1;
                }
                bytes_written += result;
                std::cout << "Bytes written to cache for block " << block_num << ": " << result << std::endl;
            }
            else
            {
                 std::cout << "Block " << block_num << " is not cached. Requesting cache invalidation." << std::endl;
                pfs::CacheRequest request;
                pfs::CacheResponse response;
                ClientContext context;
                request.set_client_id(client_id);
                request.set_filename(filename);
                request.set_block_num(block_num);
                request.set_start_offset(offset);
                request.set_end_offset(offset + num_bytes);
                Status status = metaserver_stub->InvalidateCache(&context, request, &response);
                if (response.success())
                {
                    std::cout << "Cache invalidation succeeded for block " << block_num << std::endl;
                    int result = fetchAndCacheBlock(filename, block_num, nullptr, 0, 0);
                    if (result < 0)
                    {
                        std::cerr << "Failed to fetch block " << block_num << " for writing" << std::endl;
                        return -1;
                    }

                    // Write to the fetched block in the cache
                    result = client_cache->writeToCache(
                        filename, block_num,
                        static_cast<const char *>(buf) + bytes_written,
                        block_offset, bytes_this_block);

                    if (result < 0)
                    {
                        std::cerr << "Error writing to cache after fetching block " << block_num << std::endl;
                        return -1;
                    }

                    // block_info.is_dirty = true; // Mark the block as modified
                    bytes_written += result;
                     std::cout << "Bytes written to cache for block " << block_num << ": " << result << std::endl;
                }
            }
            // if(client_cache.)
            // if write miss i.e. the blocks not in cache, we should fetch the block but before that
            //  we should invalidate the cache of the client who has the block also before invalidating we should write back the block
            // else if cache hit, write to the blocks
        }
        token_manager->CompleteTokenRange(filename, offset, offset + num_bytes);
         std::cout << "Write operation completed for range: " << offset << " to " << offset + num_bytes << std::endl;
        return bytes_written;
    }
    else if (RequestToken(fd, offset, offset + num_bytes, 1))
    {
        std::cout << "Write token requested and acquired for range: " << offset << " to " << offset + num_bytes << std::endl;
        std::string filename = fd_to_filename[fd];
        int32_t start_block = offset / PFS_BLOCK_SIZE;
        int32_t end_block = (offset + num_bytes - 1) / PFS_BLOCK_SIZE;

        for (int block_num = start_block; block_num <= end_block; block_num++)
        {
             std::cout << "Processing block " << block_num << std::endl;
            auto &block_info = block_cache_status[filename][block_num];
            size_t block_offset = (block_num == start_block) ? offset % PFS_BLOCK_SIZE : 0;
            size_t bytes_this_block = std::min(
                num_bytes - bytes_written,    // Remaining bytes to write
                PFS_BLOCK_SIZE - block_offset // Bytes till end of this block
            );
            if (client_cache->isBlockCached(filename, block_num))
            {
                std::cout << "Block " << block_num << " is cached. Writing to cache." << std::endl;
                int result = client_cache->writeToCache(
                    filename, block_num,
                    static_cast<const char *>(buf) + bytes_written,
                    block_offset, bytes_this_block);

                if (result < 0)
                {
                    std::cerr << "Error writing to cache for block " << block_num << std::endl;
                    return -1;
                }

                // block_info.is_dirty = true; // Mark the block as modified
                bytes_written += result;
                 std::cout << "Bytes written to cache for block " << block_num << ": " << result << std::endl;
            }
            else
            {
                std::cout << "Block " << block_num << " is not cached. Fetching block." << std::endl;
                int result = fetchAndCacheBlock(filename, block_num, nullptr, 0, 0);
                if (result < 0)
                {
                    std::cerr << "Failed to fetch block " << block_num << " for writing" << std::endl;
                    return -1;
                }

                // Write to the fetched block in the cache
                result = client_cache->writeToCache(
                    filename, block_num,
                    static_cast<const char *>(buf) + bytes_written,
                    block_offset, bytes_this_block);

                if (result < 0)
                {
                    std::cerr << "Error writing to cache after fetching block " << block_num << std::endl;
                    return -1;
                }

                // block_info.is_dirty = true; // Mark the block as modified
                bytes_written += result;
                 std::cout << "Bytes written to cache for block " << block_num << ": " << result << std::endl;
            }
        }
        token_manager->CompleteTokenRange(filename, offset, offset + num_bytes);
        std::cout << "Write operation completed for range: " << offset << " to " << offset + num_bytes << std::endl;
        return bytes_written;
        // we should fetch the block but before that
        // we should invalidate the cache of the client who has the block also before invalidating we should write back the block
    }
    std::cerr << "Failed to complete write operation." << std::endl;
    return -1;
}

int pfs_printAllTokensFromServer()
{
    pfs::GetAllTokensRequest request;
    pfs::GetAllTokensResponse response;
    grpc::ClientContext context;

    Status status = metaserver_stub->GetAllTokens(&context, request, &response);

    if (!status.ok())
    {
        std::cerr << "Failed to get tokens from MetaServer: " << status.error_message() << std::endl;
        return -1;
    }

    std::cout << "Tokens held by all clients:" << std::endl;
    for (const auto &token_info : response.tokens())
    {
        std::cout << "Client ID: " << token_info.client_id()
                  << ", Range: [" << token_info.start_offset() << ", " << token_info.end_offset() << "]"
                  << ", Type: " << (token_info.is_write() ? "Write" : "Read")
                  << std::endl;
    }
    return 0;
}

int pfs_close(int fd) {
    std::cout << "pfs_close called with file descriptor: " << fd << std::endl;

    // Step 1: Validate file descriptor
    if (fd < 0) {
        std::cerr << "Error: Invalid file descriptor for pfs_close: " << fd << std::endl;
        return -1;
    }

    // Check if the file descriptor exists
    auto it = fd_to_filename.find(fd);
    if (it == fd_to_filename.end()) {
        std::cerr << "Error: File descriptor not found or file is not open: " << fd << std::endl;
        return -1;
    }

    std::string filename = it->second;
    std::cout << "File associated with descriptor " << fd << " is: " << filename << std::endl;

    // Step 2: Notify the metadata server
    pfs::CloseFileRequest request;
    request.set_file_descriptor(fd);
    std::cout << "Sending CloseFileRequest to metadata server for descriptor: " << fd << std::endl;

    pfs::CloseFileResponse response;
    grpc::ClientContext context;

    grpc::Status status = metaserver_stub->CloseFile(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "Error: gRPC call to CloseFile failed with message: " << status.error_message() << std::endl;
        return -1;
    }

    if (!response.success()) {
        std::cerr << "Error: Metadata server failed to close file descriptor " << fd
                  << " with error: " << response.error_message() << std::endl;
        return -1;
    }

    std::cout << "File descriptor " << fd << " successfully closed on metadata server." << std::endl;

    // Step 3: Remove file descriptor from local tracking structures
    std::cout << "Removing file descriptor " << fd << " from local tracking structures." << std::endl;
    fd_to_filename.erase(fd);

    if (file_mode.find(fd) != file_mode.end()) {
        file_mode.erase(fd);
        std::cout << "File mode for descriptor " << fd << " removed successfully." << std::endl;
    } else {
        std::cout << "Warning: No file mode entry found for descriptor " << fd << "." << std::endl;
    }

    std::cout << "pfs_close completed successfully for file descriptor: " << fd << std::endl;
    return 0;
}

int pfs_delete(const char *filename)
{

    return 0;
}


int pfs_fstat(int fd, pfs_metadata* metadata) {
    if (fd < 0 || metadata == nullptr) {
        std::cerr << "Invalid arguments to pfs_fstat: fd=" << fd << ", metadata=" << metadata << std::endl;
        return -1;
    }

    // Map file descriptor to filename
    std::string filename = fd_to_filename[fd];
    if (filename.empty()) {
        std::cerr << "File descriptor " << fd << " is not associated with a file." << std::endl;
        return -1;
    }

    // Prepare the gRPC request
    pfs::FetchMetadataRequest request;
    request.set_file_descriptor(fd);

    pfs::FetchMetadataResponse response;
    grpc::ClientContext context;

    // Call FetchMetadata RPC using the stub
    grpc::Status status = metaserver_stub->FetchMetadata(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "Error calling FetchMetadata: " << status.error_message() << std::endl;
        return -1;
    }

    if (!response.success()) {
        std::cerr << "FetchMetadata failed: " << response.error_message() << std::endl;
        return -1;
    }

    // Populate the metadata structure
    strncpy(metadata->filename, response.filename().c_str(), sizeof(metadata->filename) - 1);
metadata->filename[sizeof(metadata->filename) - 1] = '\0'; // Ensure null-termination

    metadata->file_size = response.file_size();
    metadata->ctime = response.creation_time();
    metadata->recipe.stripe_width = response.stripe_width();

    std::cout << "Fetched metadata for fd " << fd << ":"
              << "\n  Filename: " << metadata->filename
              << "\n  File Size: " << metadata->file_size
              << "\n  Creation Time: " << metadata->ctime
              << "\n  Stripe Width: " << metadata->recipe.stripe_width << std::endl;

    return 0;
}

int pfs_execstat(struct pfs_execstat *execstat_data)
{

    return 0;
}
