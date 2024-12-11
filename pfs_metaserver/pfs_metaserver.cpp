#include "pfs_metaserver.hpp"
#include <grpcpp/grpcpp.h>
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
#include <queue>
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfs;
#define DEBUG_LOG(msg)                                                       \
    {                                                                        \
        std::ostringstream debug_oss;                                        \
        debug_oss << "[DEBUG] " << __FILE__ << ":" << __LINE__ << " " << msg; \
        std::cout << debug_oss.str() << std::endl;                           \
    }
struct FileMetadata
{
    std::string filename;
    std::int32_t file_size;
    std::int32_t creation_time;
    std::int32_t close_time;
    std::int32_t stripe_width;
    std::int32_t fd;
    std::int32_t file_mode;
    // int32_t stripe_blocks;
    //  std::map<int32_t, std::string> file_recipe;
};

// In-memory data structures
std::unordered_map<std::string, FileMetadata> metadata_store;
std::unordered_map<std::string, int32_t> filename_to_fd;
std::unordered_map<int32_t,std::string> fd_to_filename; // For fileserver mapping
std::unordered_map<std::int32_t, int32_t> file_descriptors;
std::int32_t num_servers = 0;
std::int32_t next_fd = 1;

std::mutex token_mutex;

class MetaServerImpl final : public MetaServer::Service
{
private:
    std::atomic<std::int32_t> next_client_id{1};
    std::mutex metadata_mutex;
    std::mutex clients_mutex;
    struct BlockTokenInfo
    {
        int32_t block_number;
        int num_read_tokens;
        int num_write_tokens;
        std::set<int32_t> clients_with_write_tokens;
        std::unordered_map<int32_t,int32_t>write_block_cached_client;
        bool is_cacheable;
        std::set<int32_t> clients_caching_block;

        BlockTokenInfo() : block_number(0), num_read_tokens(0), num_write_tokens(0), is_cacheable(true) {}
    };

    struct FileBlockMap
    {
        std::unordered_map<int32_t, BlockTokenInfo> blocks; // block_number -> BlockTokenInfo
        int32_t stripe_width;
    };
    std::unordered_map<std::string, FileBlockMap> file_block_tokens;
    struct ClientInfo
    {
        std::string hostname;
        std::time_t last_active;
        std::mutex stream_mutex;
        grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream;
        bool is_connected;

        explicit ClientInfo(const std::string &host)
            : hostname(host),
              last_active(std::time(nullptr)),
              stream(nullptr),
              is_connected(false)
        {
        }
    };
    std::map<std::int32_t, ClientInfo> connected_clients;

    struct TokenRange
    {
        std::int32_t client_id;
        std::string filename;
        off_t start_offset;
        off_t end_offset;
        bool is_write;
    };
    std::map<std::string, std::vector<TokenRange>> file_tokens;

     struct TokenRequestState
    {
        std::condition_variable cv_invalidate;
        std::condition_variable cv_grant;
        std::mutex mutex;
        bool invalidate_acks_received{false};
        bool grant_ack_received{false};
        std::map<std::int32_t, bool> received_invalidate_acks;
    };

    // Map to track active token requests and their states
    std::unordered_map<std::string, std::shared_ptr<TokenRequestState>> active_requests;
    std::mutex active_requests_mutex;

public:
     void PrintAllTokens(const std::string &filename) {
        std::lock_guard<std::mutex> lock(token_mutex);
        
        if (file_tokens.empty()) {
            std::cout << "\n=== No active tokens ===\n" << std::endl;
            return;
        }

        std::cout << "\n=== Current Token Distribution ===\n";
        
        // Group tokens by client_id
        std::map<std::int32_t, std::vector<TokenRange>> tokens_by_client;
        auto &tokens = file_tokens[filename];
        for (const auto& token : tokens) {
            tokens_by_client[token.client_id].push_back(token);
        }

        // Print tokens for each client
        for (const auto& [client_id, tokens] : tokens_by_client) {
            std::cout << "\nClient " << client_id << " holds " 
                     << tokens.size() << " tokens:\n";
            
            for (const auto& token : tokens) {
                std::cout<< "    Range: [" << token.start_offset 
                         << " - " << token.end_offset << "]\n"
                         << "    Type: " << (token.is_write ? "Write" : "Read")
                         << "\n";
            }
        }
        std::cout << "\n==================================\n" << std::endl;
    }

    Status RegisterClient(ServerContext *context,
                          const RegisterClientRequest *request,
                          RegisterClientResponse *response) override
    {
        std::lock_guard<std::mutex> lock(clients_mutex);

        std::int32_t client_id = next_client_id++;

        ClientInfo client_info{
            request->hostname(),
        };
        connected_clients.try_emplace(client_id, request->hostname());
        response->set_client_id(client_id);
        response->set_success(true);

        // std::cout << "Client " << client_id << " registered from "
                //   << request->hostname() << std::endl;

        return Status::OK;
    }

    Status GetAllTokens(ServerContext *context,
                        const GetAllTokensRequest *request,
                        GetAllTokensResponse *response) override
    {
        std::lock_guard<std::mutex> lock(token_mutex); // Ensure thread safety
        std::cout<<"its on the server side"<<std::endl;
        for (const auto &[filename, tokens] : file_tokens)
        {
            for (const auto &token : tokens)
            {
                pfs::TokenRequest *token_info = response->add_tokens();
                token_info->set_client_id(token.client_id);
                token_info->set_file_descriptor(filename_to_fd[filename]);
                token_info->set_start_offset(token.start_offset);
                token_info->set_end_offset(token.end_offset);
                token_info->set_is_write(token.is_write);
            }
        }

        return grpc::Status::OK;
    }
    Status RequestToken(ServerContext *context,
                        const TokenRequest *request,
                        TokenResponse *response) override
    {
        DEBUG_LOG("Token request received. Client ID: " + std::to_string(request->client_id()) +
              ", File Descriptor: " + std::to_string(request->file_descriptor()) +
              ", Start Offset: " + std::to_string(request->start_offset()) +
              ", End Offset: " + std::to_string(request->end_offset()) +
              ", Is Write: " + (request->is_write() ? "true" : "false"));
        auto filename = getTheFileName(request->file_descriptor());
        auto client_it = connected_clients.find(request->client_id());
        if (client_it == connected_clients.end())
        {
            DEBUG_LOG("Client ID not found: " + std::to_string(request->client_id()));
            response->set_success(false);
           return grpc::Status(grpc::StatusCode::NOT_FOUND, "Resource not found");
        }
        auto cacheable_blocks = HandleTokenRequest(request->client_id(), filename,
                           request->start_offset(), request->end_offset(),
                           request->is_write(), client_it->second.stream);
        for (int32_t block_num : cacheable_blocks)
        {
             DEBUG_LOG("Cacheable block added to response: " + std::to_string(block_num));
            response->add_cacheable_blocks(block_num);
        }
        response->set_filename(filename);
        response->set_client_id(request->client_id());
        response->set_success(true);
         DEBUG_LOG("Token granted successfully for client ID: " + std::to_string(request->client_id()));
        return Status::OK;
    }

    Status InvalidateCache(grpc::ServerContext *context, const CacheRequest *request,  CacheResponse *response) override{
          DEBUG_LOG("Invalidation request received. Client ID: " + std::to_string(request->client_id()) +
              ", Filename: " + request->filename() + ", Block Number: " + std::to_string(request->block_num()));
        std::int32_t client_id = request->client_id();
        std::int32_t start = request->start_offset();
        std::int32_t end = request->end_offset();
        std::string filename = request->filename();
        std::string request_id = GenerateRequestId(filename, start, end, client_id);
        auto request_state = std::make_shared<TokenRequestState>();

        {
            std::lock_guard<std::mutex> lock(active_requests_mutex);
            active_requests[request_id] = request_state;
        }
        //  std::int32_t client_id = request.client_id();
         std::int32_t block_num = request->block_num();

         //  InvalidateCacheInClient(request->client_id(), filename, block_num, client_it->second.stream);
        
         auto &file_map = file_block_tokens[filename];
         auto &block_info = file_map.blocks[block_num];
         std::int32_t clientCached = block_info.write_block_cached_client[block_num];
         auto client_it = connected_clients.find(clientCached);
        grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream = client_it->second.stream;

        StreamResponse invalidate_msg;
        invalidate_msg.set_action("invalidate");
        invalidate_msg.set_file_descriptor(filename_to_fd[filename]);
        invalidate_msg.set_filename(filename);
        invalidate_msg.set_invalidate(true);
        invalidate_msg.set_request_id(request_id);
        invalidate_msg.set_client_id(client_id);
        invalidate_msg.add_invalidate_blocks(block_num);
        invalidate_msg.set_mode(true);

        stream->Write(invalidate_msg);

        {
            std::unique_lock<std::mutex> lock(request_state->mutex);
            request_state->cv_invalidate.wait(lock, [&request_state]()
                                              {std::cout<<"got the signal and cv is released in the invalidate ack"<<std::endl; return request_state->invalidate_acks_received;});
        }

        std::int32_t key_to_remove = block_num;

        auto it = block_info.write_block_cached_client.find(key_to_remove);
        if (it != block_info.write_block_cached_client.end())
        {
            block_info.write_block_cached_client.erase(it); // Remove by iterator
            std::cout << "Removed key: " << key_to_remove << std::endl;
        }
        else
        {
            std::cout << "Key not found: " << key_to_remove << std::endl;
        }
        {
            std::lock_guard<std::mutex> lock(active_requests_mutex);
            active_requests.erase(request_id);
        }

        response->set_success(true);
         DEBUG_LOG("Cache invalidated successfully for block number: " + std::to_string(request->block_num()));
        return Status::OK;
    }

    Status ClientStream(ServerContext *context,
                        grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream) override
    {
        StreamRequest request;
        if (!stream->Read(&request))
        {
            std::cerr << "Failed to read the initial request from the stream." << std::endl;
            return Status::CANCELLED;
        }
        std::int32_t client_id = request.client_id();
        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            auto client_it = connected_clients.find(client_id);
            if (client_it != connected_clients.end())
            {
                // Store the stream in the ClientInfo structure
                client_it->second.stream = stream;
                // std::cout << "Stored stream for client " << client_id << std::endl;
            }
            else
            {
                std::cerr << "Client ID " << client_id << " not found in connected_clients map." << std::endl;
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "Client ID not found");
            }
            // std::lock_guard<std::mutex> stream_lock(client_it->second.stream_mutex);
            // client_it->second.stream = stream;
            // client_it->second.is_connected = true;
            // client_it->second.last_active = std::time(nullptr);
        }
        // std::cout << "Waiting for the request from the client" << std::endl;
        // std::atomic<bool> waiting_for_ack{false};
        // std::mutex ack_mutex;
        // std::map<std::int32_t, bool> received_acks;
        while (stream->Read(&request))
        {
                std::string request_id = request.request_id();
                std::shared_ptr<TokenRequestState> request_state;

                {
                    std::lock_guard<std::mutex> lock(active_requests_mutex);
                    auto it = active_requests.find(request_id);
                    if (it != active_requests.end())
                    {
                        std::cout<<"Inside the activerequest if condiiton seting the request state"<<std::endl;
                        request_state = it->second;
                    }
                }

                if (request_state)
                {
                    std::lock_guard<std::mutex> lock(request_state->mutex);
                    if (request.action() == "invalidate_ack")
                    {
                        request_state->received_invalidate_acks[request.client_id()] = true;

                        bool all_invalidate_acks_received = std::all_of(
                            request_state->received_invalidate_acks.begin(),
                            request_state->received_invalidate_acks.end(),
                            [](const auto &p)
                            { return p.second; });

                        if (all_invalidate_acks_received)
                        {
                            std::cout<<"Sending the signal to the invalidate ack as it has recieved ack from al the clients"<<std::endl;
                            request_state->invalidate_acks_received = true;
                            request_state->cv_invalidate.notify_one();
                        }
                    }
                    else if (request.action() == "grant_ack")
                    {
                        std::cout<<"Sending the signal to the grant ack as it has recieved ack from that particular client"<<std::endl;
                        request_state->grant_ack_received = true;
                        request_state->cv_grant.notify_one();
                    }
                }
                //wait for the client to send the invalidate ack
                //once it gets the ack notify the cv it's waiting for 
            // if(request.action() == 'grant_ack'){
            //     //wait for the client to send the ack
            //     //once it gets the ack notify the cv it's working
            // }

            {
                std::lock_guard<std::mutex> lock(clients_mutex);
                auto client_it = connected_clients.find(request.client_id());
                if (client_it != connected_clients.end())
                {
                    client_it->second.last_active = std::time(nullptr);
                }
            }
            // std::cout << "Server got request from the client with client id " << request.client_id() <<" and calls handletoken to grant the token"<< std::endl;
            // std::int32_t client_id = request.client_id();
            // std::int32_t fd = request.file_descriptor();
            // off_t start = request.start_offset();
            // off_t end = request.end_offset();
            // bool is_write = request.is_write();

            // std::string filename = getTheFileName(fd);
            // // std::cout << "The file name is -----------------" << filename << std::endl;
            // HandleTokenRequest(client_id, filename, start, end, is_write, stream, waiting_for_ack, received_acks);
        }

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            auto client_it = connected_clients.find(client_id);
            if (client_it != connected_clients.end())
            {
                std::lock_guard<std::mutex> stream_lock(client_it->second.stream_mutex);
                client_it->second.stream = nullptr;
                client_it->second.is_connected = false;
            }
        }
        return Status::OK;
    }

private:
    std::vector<int32_t> updateBlockTokens(const std::string &filename, off_t start, off_t end, bool is_write, int32_t client_id)
    {
        std::vector<int32_t> cacheable_blocks;
        auto &file_map = file_block_tokens[filename];
        int32_t start_block = start / PFS_BLOCK_SIZE;
        int32_t end_block = end / PFS_BLOCK_SIZE;

        for (int32_t block_num = start_block; block_num <= end_block; block_num++)
        {
            auto &block_info = file_map.blocks[block_num];
            block_info.block_number = block_num;

            if (is_write)
            {
                block_info.num_write_tokens++;
                block_info.clients_with_write_tokens.insert(client_id);
                block_info.is_cacheable = true;
            }
            else
            {
                block_info.num_read_tokens++;
                if (block_info.num_write_tokens == 0)
                {
                    block_info.is_cacheable = true;
                    block_info.clients_caching_block.insert(client_id);
                    cacheable_blocks.push_back(block_num);
                }
            }
        }

        return cacheable_blocks;
    }
    std::string getTheFileName(int32_t fd)
    {
        for (const auto &pair : filename_to_fd)
        {
            if (pair.second == fd)
                return pair.first;
        }
        return NULL;
    }

    std::string GenerateRequestId(const std::string &filename, off_t start, off_t end, std::int32_t client_id)
    {
        return filename + ":" + std::to_string(start) + ":" + std::to_string(end) + ":" + std::to_string(client_id) + ":" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    std::vector<int32_t> HandleTokenRequest(std::int32_t client_id, const std::string &filename,
                            off_t start, off_t end, bool is_write,
                            grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream)
    {
        std::string request_id = GenerateRequestId(filename, start, end, client_id);
        auto request_state = std::make_shared<TokenRequestState>();
        std::cout << "HandleTokenRequest: client_id=" << client_id << ", filename=" << filename
              << ", start=" << start << ", end=" << end << ", is_write=" << is_write << std::endl;

        {
            std::lock_guard<std::mutex> lock(active_requests_mutex);
            active_requests[request_id] = request_state;
        }
        auto &tokens = file_tokens[filename];
        std::vector<TokenRange> conflicting_tokens;
         {
            std::lock_guard<std::mutex> lock(clients_mutex);
            std::cout << "Checking for conflicts in tokens for filename: " << filename << std::endl;

            // Check for conflicts
            for (const auto &token : tokens)
            {
                if (TokensConflict(token, start, end, is_write))
                {
                    std::cout << "Checking for conflicts in tokens for filename: " << filename << std::endl;
                    conflicting_tokens.push_back(token);
                }
            }
        }

         std::vector<int32_t> cacheable_blocks;

        if (conflicting_tokens.empty())
        {
            // No conflicts - grant token
            std::cout << "No conflicts found, proceeding to update blocks and grant token." << std::endl;
            auto cacheable_blocks = updateBlockTokens(filename, start, end, is_write, client_id);
            TokenRange new_token{client_id, filename, start, end, is_write};
            std::cout<<"Token added on the server side from the offset "<<start<<" to "<<end<<"with the token "<<is_write<<std::endl;
            tokens.push_back(new_token);
            if(is_write){
                int32_t start_block = start / PFS_BLOCK_SIZE;
                int32_t end_block = end / PFS_BLOCK_SIZE;
                auto &file_map = file_block_tokens[filename];

                 std::cout << "Writing to blocks between " << start_block << " and " << end_block << std::endl;
                for (int32_t block_num = start_block; block_num <= end_block; block_num++)
                {
                    auto &block_info = file_map.blocks[block_num];

                    if(block_info.clients_with_write_tokens.size()>1 || block_info.clients_caching_block.size()){
                        StreamResponse invalidate_msg;
                        invalidate_msg.set_action("invalidate");
                        invalidate_msg.set_file_descriptor(filename_to_fd[filename]);
                        invalidate_msg.set_filename(filename);
                        invalidate_msg.set_invalidate(true);
                        invalidate_msg.set_request_id(request_id);
                        invalidate_msg.add_invalidate_blocks(block_num);
                        invalidate_msg.set_client_id(client_id);
                        invalidate_msg.set_mode(is_write);

                        std::cout << "Sending invalidate message for block " << block_num << std::endl;

                        for (int32_t client_id1 : block_info.clients_caching_block)
                        {
                            if(client_id1 == client_id){
                                continue;
                            }
                            auto client_it = connected_clients.find(client_id1);
                            if (client_it != connected_clients.end())
                            {
                                client_it->second.stream->Write(invalidate_msg);
                                std::cout << "Invalidate message sent to client_id: " << client_id1 << std::endl;
                            }
                        }
                        if (block_info.clients_with_write_tokens.size())
                        {
                            int32_t client_id = *block_info.clients_caching_block.begin();
                            auto client_it = connected_clients.find(client_id);
                            if (client_it != connected_clients.end())
                            {
                                client_it->second.stream->Write(invalidate_msg);
                                std::cout << "Invalidate message sent to client_id: " << client_id << std::endl;
                            }
                        }
                        {
                            std::unique_lock<std::mutex> lock(request_state->mutex);
                            request_state->cv_invalidate.wait(lock, [&request_state]()
                                                              {std::cout<<"got the signal and cv is released in the invalidate ack"<<std::endl; return request_state->invalidate_acks_received; });
                        }

                        {
                            std::lock_guard<std::mutex> lock(active_requests_mutex);
                            active_requests.erase(request_id);
                        }
                    }
                     block_info.clients_caching_block.clear();
                }
               
            }

            StreamResponse response;
            response.set_action("grant");
            response.set_file_descriptor(filename_to_fd[filename]);
            response.set_filename(filename);
            response.set_start_offset(start);
            response.set_end_offset(end);
            response.set_is_write(is_write);
            response.set_request_id(request_id);
            std::cout << "server sending the grant token permission to the client " << response.action() << std::endl;
            for (int32_t block_num : cacheable_blocks)
            {
                response.add_cacheable_blocks(block_num);
                std::cout << "Adding cacheable block: " << block_num << std::endl;
            }
            stream->Write(response);
            //wait for the ack
             {
                std::unique_lock<std::mutex> lock(request_state->mutex);
                std::cout<<"waiting for the grant ack"<<std::endl;
                request_state->cv_grant.wait(lock, [&request_state]()
                                             {  std::cout<<"Got the signal and the cv is released"<<std::endl; return request_state->grant_ack_received; });
                                           
            }
            if(is_write){
                auto &file_map = file_block_tokens[filename];
                int32_t start_block = start / PFS_BLOCK_SIZE;
                int32_t end_block = end / PFS_BLOCK_SIZE;
                for (int32_t block_num = start_block; block_num <= end_block; block_num++){
                      auto& block_info = file_map.blocks[block_num];
                      block_info.write_block_cached_client[block_num] = client_id;
                       std::cout << "Updated block " << block_num << " with write cache client " << client_id << std::endl;
                }
            }
            PrintAllTokens(filename); 
            // Clean up request state
            {
                std::lock_guard<std::mutex> lock(active_requests_mutex);
                active_requests.erase(request_id);
            }
        }
        else
        {
            // Handle conflicts
              std::cout << "Handling conflicts for tokens." << std::endl;
            cacheable_blocks = HandleTokenConflicts(conflicting_tokens, client_id, filename,
                                 start, end, is_write, stream, request_state, request_id);
        }
        return cacheable_blocks;
    }
    void removeBlockTokens(const std::string &filename, off_t start, off_t end, int32_t client_id)
    {
        auto& file_map = file_block_tokens[filename];
        auto& all_tokens = file_tokens[filename];
        int32_t start_block = start / PFS_BLOCK_SIZE;
        int32_t end_block = end / PFS_BLOCK_SIZE;

         std::cout << "Removing block tokens from " << start_block << " to " << end_block << " for filename: " << filename << std::endl;
        
        for (int32_t block_num = start_block; block_num <= end_block; block_num++) {
            auto& block_info = file_map.blocks[block_num];
            block_info.num_read_tokens = 0;
            block_info.num_write_tokens = 0;
            block_info.clients_with_write_tokens.clear();
             std::cout << "Cleared block_info for block_num: " << block_num << std::endl;
           // block_info.clients_caching_block.clear(); 
            // block_info.is_cacheable = false;
        }

        for (const auto& token : all_tokens) {
            int32_t token_start_block = token.start_offset / PFS_BLOCK_SIZE;
            int32_t token_end_block = token.end_offset / PFS_BLOCK_SIZE;
            for (int32_t block_num = token_start_block; block_num <= token_end_block; block_num++) {
                if (block_num >= start_block && block_num <= end_block) {  
                    auto& block_info = file_map.blocks[block_num];
                    if (token.is_write) {
                        if (block_info.clients_with_write_tokens.find(token.client_id) 
                            == block_info.clients_with_write_tokens.end()) {
                            block_info.num_write_tokens++;
                        }
                        block_info.clients_with_write_tokens.insert(token.client_id);
                        std::cout << "Updated write token for block_num: " << block_num << ", client_id: " << token.client_id << std::endl;

                        // block_info.is_cacheable = false;
                    } else {
                        block_info.num_read_tokens++;
                    }
                }
            }
        }
    }

    bool TokensConflict(const TokenRange &existing, off_t start, off_t end, bool is_write)
    {
        if (existing.end_offset < start || existing.start_offset > end)
        {
            return false; // No overlap
        }
         std::cout << "Token conflict detected for range: " << existing.start_offset << " to " << existing.end_offset << std::endl;

        // Conflict for write - write or read - write
        return is_write || existing.is_write;
    }

    std::vector<int32_t> HandleTokenConflicts(const std::vector<TokenRange> &conflicting_tokens,
                              std::int32_t requesting_client_id, const std::string &filename,
                              off_t start, off_t end, bool is_write,
                              grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream,  std::shared_ptr<TokenRequestState> request_state, 
                          const std::string& request_id)
    {
         std::cout << "Handling token conflicts for request: " << request_id << std::endl;
        for (const auto& token : conflicting_tokens) {
            std::cout << "Shrinking or removing token: " 
                  << "client_id=" << token.client_id 
                  << ", filename=" << token.filename 
                  << ", start=" << token.start_offset 
                  << ", end=" << token.end_offset 
                  << std::endl;
            ShrinkOrRemoveToken(token, start, end);
            removeBlockTokens(token.filename, start, end, token.client_id); //clear and update the block tokens
        }
         std::cout << "Initializing invalidate acks for request: " << request_id << std::endl;
         {
            std::lock_guard<std::mutex> lock(request_state->mutex);
            request_state->received_invalidate_acks.clear();
            request_state->invalidate_acks_received = false;
            for (const auto &token : conflicting_tokens)
            {
                request_state->received_invalidate_acks[token.client_id] = false;
                 std::cout << "Expecting invalidate ack from client: " << token.client_id << std::endl;
            }
        }
        for (const auto& token : conflicting_tokens) {
            StreamResponse invalidate_msg;
            invalidate_msg.set_action("invalidate");
            invalidate_msg.set_file_descriptor(filename_to_fd[filename]);
            invalidate_msg.set_filename(filename);
            invalidate_msg.set_start_offset(start);
            invalidate_msg.set_end_offset(end);
            invalidate_msg.set_request_id(request_id);
            invalidate_msg.set_invalidate(false);
            invalidate_msg.set_client_id(requesting_client_id);
            invalidate_msg.set_mode(is_write);
            int32_t start_block = start / PFS_BLOCK_SIZE;
            int32_t end_block = end / PFS_BLOCK_SIZE;
            for (int32_t block_num = start_block; block_num <= end_block; block_num++)
            {
                invalidate_msg.add_invalidate_blocks(block_num);
            }

            auto client_it = connected_clients.find(token.client_id);
            if (client_it != connected_clients.end()) {
                 std::cout << "Sending invalidate message to client: " << token.client_id << std::endl;
                client_it->second.stream->Write(invalidate_msg);
            }
            else
            {
                std::cerr << "Client not found: " << token.client_id << std::endl;
            }
        }
        
         {
            std::unique_lock<std::mutex> lock(request_state->mutex);
            request_state->cv_invalidate.wait(lock, [&request_state]()
                                              {std::cout << "Waiting for invalidate ack..." << std::endl; return request_state->invalidate_acks_received; });
            std::cout << "Invalidate ack received" << std::endl;                                  
        }
        
        auto cacheable_blocks = updateBlockTokens(filename, start, end, is_write, requesting_client_id);
        TokenRange new_token{requesting_client_id, filename, start, end, is_write};
        file_tokens[filename].push_back(new_token);

        StreamResponse grant_msg;
        grant_msg.set_action("grant");
        grant_msg.set_file_descriptor(filename_to_fd[filename]);
        grant_msg.set_filename(filename);
        grant_msg.set_start_offset(start);
        grant_msg.set_end_offset(end);
        grant_msg.set_is_write(is_write);
        grant_msg.set_request_id(request_id);

        for (int32_t block_num : cacheable_blocks)
        {
            grant_msg.add_cacheable_blocks(block_num);
        }

         std::cout << "Sending grant message to client: " << requesting_client_id << std::endl;
        stream->Write(grant_msg);

        {
            std::unique_lock<std::mutex> lock(request_state->mutex);
            request_state->cv_grant.wait(lock, [&request_state]()
                                         {  std::cout << "Waiting for grant ack..." << std::endl; return request_state->grant_ack_received; });
            std::cout << "Grant ack received" << std::endl;
        }

        PrintAllTokens(filename); 
        // Clean up request state
        {
            std::lock_guard<std::mutex> lock(active_requests_mutex);
            active_requests.erase(request_id);
            std::cout << "Request state cleaned up for request: " << request_id << std::endl;
        }
        //wait for the grant ack
        return cacheable_blocks;
    }

    void ShrinkOrRemoveToken(const TokenRange &token, off_t start, off_t end)
    {
        std::cout << "In shrinking or removing token: "
              << "client_id=" << token.client_id 
              << ", filename=" << token.filename 
              << ", start=" << token.start_offset 
              << ", end=" << token.end_offset 
              << std::endl;
        // Protect file_tokens if accessed concurrently
        std::lock_guard<std::mutex> lock(clients_mutex);
        auto &tokens = file_tokens[token.filename];

        auto it = std::find_if(tokens.begin(), tokens.end(),
                               [&token](const TokenRange &t)
                               {
                                   return t.client_id == token.client_id &&
                                          t.start_offset == token.start_offset &&
                                          t.end_offset == token.end_offset;
                               });

        if (it != tokens.end())
        {
            std::cout << "Found token: " << it->start_offset << " to " << it->end_offset << std::endl;

            std::vector<TokenRange> new_tokens;
            if (start > it->start_offset)
            {
                std::cout << "Shrink on left: " << it->start_offset << " to " << start - 1 << std::endl;
                new_tokens.push_back({it->client_id, token.filename, it->start_offset, start - 1, it->is_write});
            }
            if (end < it->end_offset)
            {
                std::cout << "Shrink on right: " << end + 1 << " to " << it->end_offset << std::endl;
                new_tokens.push_back({it->client_id, token.filename, end + 1, it->end_offset, it->is_write});
            }
            tokens.erase(it);
            tokens.insert(tokens.end(), new_tokens.begin(), new_tokens.end());
            std::cout << "ShrinkorRemove happened on the server side for the client and the tokens are updated " << std::endl;
        }
        else
        {
            std::cerr << "Token not found for shrinking or removing!" << std::endl;
        }
    }

    // void CleanupClientTokens(std::int32_t client_id)
    // {
    //     std::lock_guard<std::mutex> lock(metadata_mutex);
    //     for (auto &[filename, tokens] : file_tokens)
    //     {
    //         tokens.erase(
    //             std::remove_if(tokens.begin(), tokens.end(),
    //                            [client_id](const TokenRange &t)
    //                            {
    //                                return t.client_id == client_id;
    //                            }),
    //             tokens.end());
    //     }
    // }
    // Add method to check if client is still active
    // bool IsClientActive(int client_id)
    // {
    //     std::lock_guard<std::mutex> lock(clients_mutex);
    //     auto it = connected_clients.find(client_id);
    //     if (it == connected_clients.end())
    //     {
    //         return false;
    //     }
    //     return (std::time(nullptr) - it->second.last_active) < CLIENT_TIMEOUT;
    // }
    Status CheckAliveMetaServer(ServerContext *context, const AliveRequestMeta *request,
                                AliveResponseMeta *response) override
    {
        std::cout << "Received Alive check request." << std::endl;
        response->set_alive(true);
        response->set_server_message("File server is healthy.");
        return Status::OK;
    }
    Status CreateFile(ServerContext *context, const CreateFileRequest *request,
                      CreateFileResponse *response) override
    {
         DEBUG_LOG("Received CreateFile request for filename: " + request->filename());
        std::string filename = request->filename();
        std::int32_t stripe_width = request->stripe_width();
        std::cout << "Its here in the server side create file " << std::endl;
        // Check if the file already exists
        if (!filename_to_fd.empty() && filename_to_fd.find(filename) != filename_to_fd.end())
        {
              DEBUG_LOG("File already exists: " + filename);
            response->set_success(false);
            response->set_error_message("File already exists.");
            return Status::OK;
        }

        if (stripe_width > num_servers)
        {
            response->set_success(false);
            response->set_error_message("Stripe_width for the given file is greater than number of servers");
            return Status::OK;
        }

        std::cout << "Setting metadata for the file : "<< filename << std::endl;
        // Create new file metadata
        FileMetadata meta_data;
        meta_data.filename = filename;
        meta_data.file_size = 0;
        meta_data.creation_time = std::time(nullptr);
        meta_data.fd = -1;
        meta_data.file_mode = 0;
        meta_data.stripe_width = stripe_width;

        // Store metadata and generate file descriptor
        //std::int32_t fd = next_fd++;
        metadata_store[filename] = meta_data;
        //filename_to_fd[filename] = fd;
        DEBUG_LOG("File metadata created for filename: " + filename);   
        response->set_success(true);
        //response->set_file_descriptor(fd);
        return Status::OK;
    }

    // OpenFile RPC implementation
    Status OpenFile(ServerContext *context, const OpenFileRequest *request,
                    OpenFileResponse *response) override
    {
        std::string filename = request->filename();
        std::int32_t mode = request->mode();

        // Check if the file exists in metadata_store
        auto metadata_it = metadata_store.find(filename);
        if (metadata_it == metadata_store.end()) {
            std::cout << "File does not exist in metadata_store: " << filename << std::endl;
            response->set_success(false);
            response->set_error_message("File not found.");
            return Status::OK;
        }

        // Step 3: Assign a new file descriptor
        std::int32_t fd = next_fd++;
        metadata_store[filename].fd = fd;
        metadata_store[filename].file_mode = mode;
        //std::int32_t fd = filename_to_fd[filename];
        file_descriptors[fd] = mode;
        // exec_stats.num_open_files++;
        fd_to_filename[fd]=filename;
        filename_to_fd[filename] = fd;

        response->set_file_descriptor(fd);
        response->set_success(true);
        std::cout<<response->error_message()<<std::endl;
        return Status::OK;
    }

    // FetchMetadata RPC implementation
    Status FetchMetadata(ServerContext *context, const FetchMetadataRequest *request,
                         FetchMetadataResponse *response) override
    {
        std::int32_t fd = request->file_descriptor();

        // Validate the file descriptor
        auto fd_it = fd_to_filename.find(fd);
        if (fd_it == fd_to_filename.end()) {
            response->set_success(false);
            response->set_error_message("Invalid file descriptor.");
            return Status::OK;
        }

        const std::string& filename = fd_it->second;

        // Validate metadata
        auto metadata_it = metadata_store.find(filename);
        if (metadata_it == metadata_store.end()) {
            response->set_success(false);
            response->set_error_message("Metadata not found for file.");
            return Status::OK;
        }

        const FileMetadata& file_meta = metadata_it->second;

        // Populate response
        response->set_filename(file_meta.filename);
        response->set_file_size(file_meta.file_size);
        response->set_creation_time(file_meta.creation_time);
        response->set_stripe_width(file_meta.stripe_width);

        response->set_success(true);
        return Status::OK;
    }

    // Status invalidateSet
    // UpdateMetadata RPC implementation
    Status UpdateMetadata(ServerContext *context, const UpdateMetadataRequest *request,
                          UpdateMetadataResponse *response) override
    {
        std::int32_t fd = request->file_descriptor();
        std::int32_t new_file_size = request->new_file_size();
        //std::int32_t modification_time = request->modification_time();

        // Find the filename for the given file descriptor
        auto fd_it = fd_to_filename.find(fd);
        if (fd_it == fd_to_filename.end()) {
            response->set_success(false);
            response->set_error_message("Invalid file descriptor.");
            return Status::OK; // Logical error, not a system failure
        }

        const std::string &filename = fd_it->second;

        // Check if the file exists in the metadata store
        auto metadata_it = metadata_store.find(filename);
        if (metadata_it == metadata_store.end()) {
            response->set_success(false);
            response->set_error_message("File metadata not found.");
            return Status::OK; 
        }

        // Update the file metadata
        FileMetadata &file_meta = metadata_it->second;
        file_meta.file_size = new_file_size;

        // Respond with success
        response->set_success(true);
        return Status::OK;
    }

    // DeleteFile RPC implementation
    Status DeleteFile(ServerContext *context, const DeleteFileRequest *request,
                      DeleteFileResponse *response) override
    {
        std::string filename = request->filename();

        // Check if the file exists
        if (filename_to_fd.find(filename) == filename_to_fd.end())
        {
            response->set_success(false);
            response->set_error_message("File not found.");
            return Status::OK;
        }

        std::int32_t fd = filename_to_fd[filename];
        // metadata_store.erase(fd);
        filename_to_fd.erase(filename);
        file_descriptors.erase(fd);
        response->set_success(true);
        return Status::OK;
    }

   Status CloseFile(ServerContext *context, const CloseFileRequest *request, CloseFileResponse *response) override
{
    std::lock_guard<std::mutex> metadata_lock(metadata_mutex);

    int32_t fd = request->file_descriptor();
    int32_t client_id = request->client_id();
    DEBUG_LOG("Processing CloseFile request for file descriptor: " + std::to_string(fd) + ", client ID: " + std::to_string(client_id));

    // Check if the file descriptor is valid
    auto fd_it = fd_to_filename.find(fd);
    auto mode_it = file_descriptors.find(fd);
    if (fd_it == fd_to_filename.end())
    {
        DEBUG_LOG("Invalid file descriptor: " + std::to_string(fd));
        response->set_success(false);
        response->set_error_message("Invalid file descriptor");
        return Status::OK;
    }

    std::string filename = fd_it->second;
    int mode = file_descriptors[fd];
    DEBUG_LOG("Valid file descriptor. Filename: " + filename + ", Mode: " + std::to_string(mode));

    fd_to_filename.erase(fd_it);
    file_descriptors.erase(mode_it);
    DEBUG_LOG("Erased file descriptor and associated data.");

    {
        std::lock_guard<std::mutex> lock(token_mutex);
        auto &tokens = file_tokens[filename];
        DEBUG_LOG("Cleaning tokens for filename: " + filename + ", Number of tokens before cleanup: " + std::to_string(tokens.size()));

        tokens.erase(
            std::remove_if(tokens.begin(), tokens.end(),
                           [&](const TokenRange &t)
                           { return t.client_id == client_id; }),
            tokens.end());

        if (tokens.empty())
        {
            file_tokens.erase(filename);
            DEBUG_LOG("No tokens remaining for file. Removed entry for filename: " + filename);
        }
        else
        {
            DEBUG_LOG("Tokens remaining for file after cleanup: " + std::to_string(tokens.size()));
        }
    }

    {
        if (file_block_tokens.find(filename) != file_block_tokens.end())
        {
            auto &file_map = file_block_tokens[filename];
            DEBUG_LOG("File found in block tokens. Processing blocks for file: " + filename);

            for (auto &[block_number, block_info] : file_map.blocks)
            {
                DEBUG_LOG("Processing block number: " + std::to_string(block_number));

                block_info.write_block_cached_client.erase(client_id);
                DEBUG_LOG("Removed client from write_block_cached_client for block: " + std::to_string(block_number));

                block_info.clients_caching_block.erase(client_id);
                DEBUG_LOG("Removed client from clients_caching_block for block: " + std::to_string(block_number));
            }

            if (metadata_store.find(filename) != metadata_store.end())
            {
                int32_t file_size = metadata_store[filename].file_size;
                DEBUG_LOG("File size retrieved from metadata store: " + std::to_string(file_size));
                removeBlockTokens(filename, 0, file_size, client_id);
                DEBUG_LOG("Removed block tokens for file: " + filename);
            }
            else
            {
                DEBUG_LOG("File not found in metadata store: " + filename);
            }
        }
        else
        {
            DEBUG_LOG("File not found in block tokens: " + filename);
        }
    }

    // TODO: Update block tokens for all blocks of the file, use removeBlockTokens
    // TODO: Update the last close time and handle opened_fd
    DEBUG_LOG("Completed cleanup for file: " + filename);

    response->set_success(true);
    return Status::OK;
}
};
void RunMetaServer(const std::string &server_address)
{
    DEBUG_LOG("Initializing meta server at address: " + server_address);
    MetaServerImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
     DEBUG_LOG("Meta server listening on: " + server_address);
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
    DEBUG_LOG("Meta server shut down.");
}
std::int32_t main(std::int32_t argc, char *argv[])
{
    printf("%s:%s: PFS meta server start! Hostname: %s, IP: %s\n", __FILE__,
           __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open())
    {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    std::string line;
    std::getline(pfs_list, line);
    if (line.substr(0, line.find(':')) != getMyHostname())
    {
        fprintf(stderr, "%s: hostname not on the first line of pfs_list.txt.\n",
                __func__);
        exit(EXIT_FAILURE);
    }
    std::string sline;
    while (std::getline(pfs_list, sline))
    {
        num_servers++;
        std::cout << "The number of servers is----------------------------" << num_servers << std::endl;
        if (pfs_list.eof())
            break;
    }
    pfs_list.close();

    std::string listen_port = line.substr(line.find(':') + 1);
    std::cout << "Listen port of metaserver is------------------------------------------------------0.0.0.0:" << listen_port << std::endl;

    // RunFileServer("0.0.0.0:"+listen_port);
    // std::string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS metadata server and listen to requests
    // std::string server_address = "0.0.0.0:"+line.substr(line.find(':') + 1);
    RunMetaServer("0.0.0.0:" + listen_port);

    printf("%s: Launching PFS metadata server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...

    printf("%s:%s: PFS meta server done!\n", __FILE__, __func__);
    return 0;
}
