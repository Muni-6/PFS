#include "pfs_metaserver.hpp"
#include <grpcpp/grpcpp.h>
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
#include <queue>
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfs;
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
    // std::mutex active_requests_mutex;
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
        // explicit ClientInfo(const std::string &host)
        //     : hostname(host),
        //       last_active(std::time(nullptr)), // Initialize time to now
        //       stream(nullptr),                 // Initialize stream to null
        //       is_connected(false)
        // {
        // } // Initialize connection status to false

        // // Delete copy constructor due to mutex member
        // ClientInfo(const ClientInfo &) = delete;
        // // Delete assignment operator due to mutex member
        // ClientInfo &operator=(const ClientInfo &) = delete;
        // // Allow move construction
        // ClientInfo(ClientInfo &&) = default;
        // // Allow move assignment
        // ClientInfo &operator=(ClientInfo &&) = default;
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
     void PrintAllTokens() {
        std::lock_guard<std::mutex> lock(token_mutex);
        
        if (file_tokens.empty()) {
            std::cout << "\n=== No active tokens ===\n" << std::endl;
            return;
        }

        std::cout << "\n=== Current Token Distribution ===\n";
        
        // Group tokens by client_id
        std::map<std::int32_t, std::vector<TokenRange>> tokens_by_client;
        auto &tokens = file_tokens["pfs_file1"];
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
        auto filename = getTheFileName(request->file_descriptor());
        auto client_it = connected_clients.find(request->client_id());
        HandleTokenRequest(request->client_id(), filename,
                           request->start_offset(), request->end_offset(),
                           request->is_write(), client_it->second.stream );
        response->set_client_id(request->client_id());
        response->set_success(true);
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
        // Generate a unique request ID based on filename, offsets, and client_id
        return filename + ":" + std::to_string(start) + ":" + std::to_string(end) + ":" + std::to_string(client_id) + ":" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    void HandleTokenRequest(std::int32_t client_id, const std::string &filename,
                            off_t start, off_t end, bool is_write,
                            grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream)
    {
        std::string request_id = GenerateRequestId(filename, start, end, client_id);
        auto request_state = std::make_shared<TokenRequestState>();

        {
            std::lock_guard<std::mutex> lock(active_requests_mutex);
            active_requests[request_id] = request_state;
        }
        auto &tokens = file_tokens[filename];
        std::vector<TokenRange> conflicting_tokens;
         {
            std::lock_guard<std::mutex> lock(clients_mutex);

            // Check for conflicts
            for (const auto &token : tokens)
            {
                if (TokensConflict(token, start, end, is_write))
                {
                    conflicting_tokens.push_back(token);
                }
            }
        }

        if (conflicting_tokens.empty())
        {
            // No conflicts - grant token
            TokenRange new_token{client_id, filename, start, end, is_write};
            std::cout<<"Token added on the server side from the offset "<<start<<" to "<<end<<"with the token "<<is_write<<std::endl;
            tokens.push_back(new_token);

            StreamResponse response;
            response.set_action("grant");
            response.set_file_descriptor(filename_to_fd[filename]);
            response.set_filename(filename);
            response.set_start_offset(start);
            response.set_end_offset(end);
            response.set_is_write(is_write);
            response.set_request_id(request_id);
            std::cout << "server sending the grant token permission to the client " << response.action() << std::endl;
            stream->Write(response);
            //wait for the ack
             {
                std::unique_lock<std::mutex> lock(request_state->mutex);
                std::cout<<"waiting for the grant ack"<<std::endl;
                request_state->cv_grant.wait(lock, [&request_state]()
                                             { return request_state->grant_ack_received; });
                std::cout<<"Got the signal and the cv is released"<<std::endl;                             
            }

            PrintAllTokens(); 
            // Clean up request state
            {
                std::lock_guard<std::mutex> lock(active_requests_mutex);
                active_requests.erase(request_id);
            }
        }
        else
        {
            // Handle conflicts
            std::cout<<"In handling conflict tokens"<<std::endl;
            HandleTokenConflicts(conflicting_tokens, client_id, filename,
                                 start, end, is_write, stream, request_state, request_id);
        }
    }

    bool TokensConflict(const TokenRange &existing, off_t start, off_t end, bool is_write)
    {
        if (existing.end_offset < start || existing.start_offset > end)
        {
            return false; // No overlap
        }

        // Conflict for write - write or read - write
        return is_write || existing.is_write;
    }

    void HandleTokenConflicts(const std::vector<TokenRange> &conflicting_tokens,
                              std::int32_t requesting_client_id, const std::string &filename,
                              off_t start, off_t end, bool is_write,
                              grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream,  std::shared_ptr<TokenRequestState> request_state, 
                          const std::string& request_id)
    {
         {
            std::lock_guard<std::mutex> lock(request_state->mutex);
            request_state->received_invalidate_acks.clear();
            request_state->invalidate_acks_received = false;
            for (const auto &token : conflicting_tokens)
            {
                request_state->received_invalidate_acks[token.client_id] = false;
            }
        }
        // received_acks.clear();
        // for (const auto &token : conflicting_tokens)
        // {
        //     received_acks[token.client_id] = false; // Set ack as false for each conflicting token
        // }

        // Send invalidations
        for (const auto& token : conflicting_tokens) {
            StreamResponse invalidate_msg;
            invalidate_msg.set_action("invalidate");
            invalidate_msg.set_file_descriptor(filename_to_fd[filename]);
            invalidate_msg.set_filename(filename);
            invalidate_msg.set_start_offset(start);
            invalidate_msg.set_end_offset(end);
            invalidate_msg.set_request_id(request_id);

            auto client_it = connected_clients.find(token.client_id);
            if (client_it != connected_clients.end()) {
                client_it->second.stream->Write(invalidate_msg);
            }
        }
        
         {
            std::unique_lock<std::mutex> lock(request_state->mutex);
            request_state->cv_invalidate.wait(lock, [&request_state]()
                                              {std::cout<<"got the signal and cv is released in the invalidate ack"<<std::endl; return request_state->invalidate_acks_received; });
        }

        for (const auto& token : conflicting_tokens) {
            ShrinkOrRemoveToken(token, start, end);
        }
        // Wait for acks with timeout
        // auto start_time = std::chrono::steady_clock::now();
        // while (waiting_for_ack.load()) {
        //     auto current_time = std::chrono::steady_clock::now();
        //     if (std::chrono::duration_cast<std::chrono::seconds>(
        //             current_time - start_time).count() > 5) {
        //         std::cout << "Timeout waiting for acks" << std::endl;
        //         return;
        //     }
        //     std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // }
        // std::cout<<"Got ACK from all the clients and moving on to the shrinkorremove token"<<std::endl;
        // // Process token updates and grant new token
        //wait for all the acks

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
        stream->Write(grant_msg);

          {
            std::unique_lock<std::mutex> lock(request_state->mutex);
            request_state->cv_grant.wait(lock, [&request_state]()
                                         {std::cout<<"got the signal and cv is released in the grant ack at 2nd place"<<std::endl; return request_state->grant_ack_received; });
        }

        PrintAllTokens(); 
        // Clean up request state
        {
            std::lock_guard<std::mutex> lock(active_requests_mutex);
            active_requests.erase(request_id);
        }
        //wait for the grant ack
    }

    void ShrinkOrRemoveToken(const TokenRange &token, off_t start, off_t end)
    {
       std::cout << "In shrinking or removing token" << std::endl;

    // Protect file_tokens if accessed concurrently
    std::lock_guard<std::mutex> lock(clients_mutex);
    auto &tokens = file_tokens[token.filename];

    auto it = std::find_if(tokens.begin(), tokens.end(),
                           [&token](const TokenRange &t) {
                               return t.client_id == token.client_id &&
                                      t.start_offset == token.start_offset &&
                                      t.end_offset == token.end_offset;
                           });

    if (it != tokens.end()) {
        std::cout << "Found token: " << it->start_offset << " to " << it->end_offset << std::endl;

        std::vector<TokenRange> new_tokens;

        // Add left portion if shrinking on the left
        if (start > it->start_offset) {
            std::cout << "Shrink on left: " << it->start_offset << " to " << start - 1 << std::endl;
            new_tokens.push_back({it->client_id, token.filename, it->start_offset, start - 1, it->is_write});
        }

        // Add right portion if shrinking on the right
        if (end < it->end_offset) {
            std::cout << "Shrink on right: " << end + 1 << " to " << it->end_offset << std::endl;
            new_tokens.push_back({it->client_id, token.filename, end + 1, it->end_offset, it->is_write});
        }

        // Erase the original token
        tokens.erase(it);

        // Add the new tokens after erasing
        tokens.insert(tokens.end(), new_tokens.begin(), new_tokens.end());
        std::cout<<"ShrinkorRemove happened on the server side for the client and the tokens are updated "<<std::endl;
    } else {
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
        std::string filename = request->filename();
        std::int32_t stripe_width = request->stripe_width();
        std::cout << "Its here in the server side create file " << std::endl;
        // Check if the file already exists
        if (!filename_to_fd.empty() && filename_to_fd.find(filename) != filename_to_fd.end())
        {
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
        //file_descriptors[fd] = mode;
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

    Status CloseFile(ServerContext* context, const CloseFileRequest* request, CloseFileResponse* response) override {
        std::lock_guard<std::mutex> lock(metadata_mutex);

        int32_t fd = request->file_descriptor();

        auto fd_it = fd_to_filename.find(fd);
        if (fd_it == fd_to_filename.end()) {
            response->set_success(false);
            response->set_error_message("Invalid or non-existent file descriptor.");
            return Status::OK;
        }

        auto &file_metadata = metadata_store[fd_it->second];
        file_metadata.fd = -1; // Reset file descriptor
        file_metadata.close_time = std::time(nullptr);


        // Perform cleanup: Remove file descriptor from fd_to_filename
        std::cout << "Closing file descriptor: " << fd
                << ", Filename: " << fd_it->second << std::endl;
        fd_to_filename.erase(fd_it);

        // Respond with success
        response->set_success(true);
        return Status::OK;
        }
};
void RunMetaServer(const std::string &server_address)
{
    MetaServerImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
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
