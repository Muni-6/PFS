#include "pfs_metaserver.hpp"
#include <grpcpp/grpcpp.h>
#include "pfs_proto/pfs_metaserver.grpc.pb.h"

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
    // int32_t stripe_blocks;
    //  std::map<int32_t, std::string> file_recipe;
};

// In-memory data structures
std::unordered_map<std::int32_t, FileMetadata> metadata_store;
std::unordered_map<std::string, int32_t> filename_to_fd;
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
    struct ClientInfo
    {
        std::string hostname;
        std::time_t last_active;
        std::mutex stream_mutex;
        grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream;
        bool is_connected;
        explicit ClientInfo(const std::string &host)
            : hostname(host),
              last_active(std::time(nullptr)), // Initialize time to now
              stream(nullptr),                 // Initialize stream to null
              is_connected(false)
        {
        } // Initialize connection status to false

        // Delete copy constructor due to mutex member
        ClientInfo(const ClientInfo &) = delete;
        // Delete assignment operator due to mutex member
        ClientInfo &operator=(const ClientInfo &) = delete;
        // Allow move construction
        ClientInfo(ClientInfo &&) = default;
        // Allow move assignment
        ClientInfo &operator=(ClientInfo &&) = default;
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

public:
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

        std::cout << "Client " << client_id << " registered from "
                  << request->hostname() << std::endl;

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
                std::cout << "Stored stream for client " << client_id << std::endl;
            }
            else
            {
                std::cerr << "Client ID " << client_id << " not found in connected_clients map." << std::endl;
                return grpc::Status(grpc::StatusCode::NOT_FOUND, "Client ID not found");
            }
            std::lock_guard<std::mutex> stream_lock(client_it->second.stream_mutex);
            client_it->second.stream = stream;
            client_it->second.is_connected = true;
            client_it->second.last_active = std::time(nullptr);
        }
        std::cout << "Waiting for the request from the client" << std::endl;
        while (stream->Read(&request))
        {
            {
                std::lock_guard<std::mutex> lock(clients_mutex);
                auto client_it = connected_clients.find(request.client_id());
                if (client_it != connected_clients.end())
                {
                    client_it->second.last_active = std::time(nullptr);
                }
            }
            std::cout << "Server got request from the client with client id " << request.client_id() << std::endl;
            std::int32_t client_id = request.client_id();
            std::int32_t fd = request.file_descriptor();
            off_t start = request.start_offset();
            off_t end = request.end_offset();
            bool is_write = request.is_write();

            std::string filename = getTheFileName(fd);
            std::cout << "The file name is -----------------" << filename << std::endl;
            // if(filename == NULL){
            // handle this
            // }
            // Process token request
            HandleTokenRequest(client_id, filename, start, end, is_write, stream);
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

    void HandleTokenRequest(std::int32_t client_id, const std::string &filename,
                            off_t start, off_t end, bool is_write,
                            grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream)
    {
        std::lock_guard<std::mutex> lock(metadata_mutex);

        auto &tokens = file_tokens[filename]; // 50 - 300 client1, fd1, write token
        std::vector<TokenRange> conflicting_tokens;

        // Check for conflicts
        for (const auto &token : tokens)
        {
            if (TokensConflict(token, start, end, is_write))
            {
                conflicting_tokens.push_back(token); // 50 - 300 of client1 is a conflicting token
            }
        }

        if (conflicting_tokens.empty())
        {
            // No conflicts - grant token
            TokenRange new_token{client_id, filename, start, end, is_write};
            tokens.push_back(new_token);

            StreamResponse response;
            response.set_action("grant");
            response.set_file_descriptor(filename_to_fd[filename]);
            response.set_filename(filename);
            response.set_start_offset(start);
            response.set_end_offset(end);
            response.set_is_write(is_write);
            std::cout << "server sending the grant token permission to the client " << response.action() << std::endl;
            stream->Write(response);
        }
        else
        {
            // Handle conflicts
            HandleTokenConflicts(conflicting_tokens, client_id, filename,
                                 start, end, is_write, stream);
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
                              grpc::ServerReaderWriter<StreamResponse, StreamRequest> *stream)
    {
        std::map<std::int32_t, ClientInfo>::iterator client_it;
        for (const auto &token : conflicting_tokens)
        {
            ShrinkOrRemoveToken(token, start, end);
            // Notify existing token holder to invalidate
            StreamResponse invalidate_msg;
            invalidate_msg.set_action("invalidate");
            invalidate_msg.set_file_descriptor(filename_to_fd[filename]);
            invalidate_msg.set_filename(filename);
            invalidate_msg.set_start_offset(start);
            invalidate_msg.set_end_offset(end);

            {
                std::lock_guard<std::mutex> lock(clients_mutex);
                client_it = connected_clients.find(token.client_id);
                if (client_it != connected_clients.end())
                {
                    std::lock_guard<std::mutex> stream_lock(client_it->second.stream_mutex);
                    std::cout << "Getting seg fault here " << std::endl;
                    if (client_it->second.is_connected && client_it->second.stream)
                    {
                        std::cout << "server sending the invalidate token permission to the respective client " << invalidate_msg.action() << std::endl;
                        client_it->second.stream->Write(invalidate_msg);
                    }
                }
            }
            // auto client_it = connected_clients.find(token.client_id);
            // if (client_it != connected_clients.end() && client_it->second.stream)
            // {
            //      std::cout<<"server sending the invalidate token permission to the respective client "<<invalidate_msg.action()<<std::endl;
            //     client_it->second.stream->Write(invalidate_msg);
            // }

            // Remove or shrink existing token
        }

        // Grant new token to requesting client
        StreamRequest ack_request;
        if (client_it->second.stream->Read(&ack_request) && ack_request.action() == "invalidate_ack")
        {
            std::cout << "Received acknowledgment from client "<<std::endl;
            TokenRange new_token{requesting_client_id, filename, start, end, is_write};
            file_tokens[filename].push_back(new_token);

            StreamResponse grant_msg;
            grant_msg.set_action("grant");
            grant_msg.set_file_descriptor(filename_to_fd[filename]);
            grant_msg.set_filename(filename);
            grant_msg.set_start_offset(start);
            grant_msg.set_end_offset(end);
            grant_msg.set_is_write(is_write);
            stream->Write(grant_msg);
        }
        else
        {
            std::cerr << "Failed to receive acknowledgment from client Proceeding without acknowledgment." << std::endl;
        }
    }

    void ShrinkOrRemoveToken(const TokenRange &token, off_t start, off_t end)
    {
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
            if (start > it->start_offset)
            {
                // Add left portion
                tokens.push_back({it->client_id, token.filename, it->start_offset, start - 1, it->is_write});
            }
            if (end < it->end_offset)
            {
                // Add right portion
                tokens.push_back({it->client_id, token.filename, end + 1, it->end_offset, it->is_write});
            }
            tokens.erase(it);
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
            response->set_error_message("stripe_width greater than number of servers");
            return Status::OK;
        }

        std::cout << "Setting metadata " << std::endl;
        // Create new file metadata
        FileMetadata meta_data;
        meta_data.filename = filename;
        meta_data.file_size = 500;
        meta_data.creation_time = std::time(nullptr);
        // meta_data.stripe_block = stripe_block;

        // Store metadata and generate file descriptor
        std::int32_t fd = next_fd++;
        metadata_store[fd] = meta_data;
        filename_to_fd[filename] = fd;

        response->set_success(true);
        return Status::OK;
    }

    // OpenFile RPC implementation
    Status OpenFile(ServerContext *context, const OpenFileRequest *request,
                    OpenFileResponse *response) override
    {
        std::string filename = request->filename();
        std::int32_t mode = request->mode();

        // Check if the file exists
        if (filename_to_fd.find(filename) == filename_to_fd.end())
        {
            response->set_success(false);
            response->set_error_message("File not found.");
            return Status::OK;
        }

        std::int32_t fd = filename_to_fd[filename];
        file_descriptors[fd] = mode;
        // exec_stats.num_open_files++;

        response->set_file_descriptor(fd);
        response->set_success(true);
        return Status::OK;
    }

    // FetchMetadata RPC implementation
    Status FetchMetadata(ServerContext *context, const FetchMetadataRequest *request,
                         FetchMetadataResponse *response) override
    {
        std::int32_t fd = request->file_descriptor();

        // Check if the file descriptor exists
        if (metadata_store.find(fd) == metadata_store.end())
        {
            response->set_success(false);
            response->set_error_message("File descriptor not found.");
            return Status::OK;
        }

        const FileMetadata &file_meta = metadata_store[fd];

        // Populate response with metadata
        response->set_filename(file_meta.filename);
        response->set_file_size(file_meta.file_size);
        response->set_creation_time(file_meta.creation_time);

        // for (const auto& [server_id, byte_range] : file_meta.file_recipe) {
        //     (*response->mutable_file_recipe())[server_id] = byte_range;
        // }

        response->set_success(true);
        return Status::OK;
    }

    // UpdateMetadata RPC implementation
    Status UpdateMetadata(ServerContext *context, const UpdateMetadataRequest *request,
                          UpdateMetadataResponse *response) override
    {
        std::int32_t fd = request->file_descriptor();
        std::int32_t new_file_size = request->new_file_size();
        std::int32_t modification_time = request->modification_time();

        // Check if the file descriptor exists
        if (metadata_store.find(fd) == metadata_store.end())
        {
            response->set_success(false);
            response->set_error_message("File descriptor not found.");
            return Status::OK;
        }

        FileMetadata &file_meta = metadata_store[fd];
        file_meta.file_size = new_file_size;
        // file_meta.modification_time = modification_time;

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
        metadata_store.erase(fd);
        filename_to_fd.erase(filename);
        file_descriptors.erase(fd);
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
