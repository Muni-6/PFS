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

class ClientTokenManager
{
private:
    std::int32_t client_id;
    std::unique_ptr<grpc::ClientContext> context_ptr;
    std::thread reader_thread;
    std::mutex tokens_mutex;
    struct TokenRange
    {
        off_t start_offset;
        off_t end_offset;
        bool is_write;
        std::string filename;
        std::int32_t fd;
    };
    std::vector<TokenRange> held_tokens;
    std::shared_ptr<grpc::ClientReaderWriter<pfs::StreamRequest, pfs::StreamResponse>> stream;

public:
    ClientTokenManager(std::int32_t id, std::shared_ptr<MetaServer::Stub> stub) : client_id(id)
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
        // cout<<"Inside Client Toke";
        reader_thread = std::thread([this]()
                                    {
            pfs::StreamResponse response;
            while (stream->Read(&response)) {
                std::cout<<"Got response from the server to the client with the action "<<response.action()<<std::endl;
                std::lock_guard<std::mutex> lock(tokens_mutex);
                
                if (response.action() == "invalidate") {
                    // Remove invalidated token range
                    RemoveTokenRange(response.filename(), 
                                   response.start_offset(), 
                                   response.end_offset());
                    std::cout << "Client invalidated token for file: " << response.filename() << std::endl;

            // Send acknowledgment back to the server
            pfs::StreamRequest ack_request;
            // ack_response.set_client_id(client_id);
            ack_request.set_action("invalidate_ack");
            ack_request.set_file_descriptor(response.file_descriptor());
            // ack_request.set_filename(response.filename());
            ack_request.set_start_offset(response.start_offset());
            ack_request.set_end_offset(response.end_offset());

            if (!stream->Write(ack_request)) {
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
                }
            }  std::cout << "Reader thread ending" << std::endl; });
        reader_thread.detach();
    }

    bool HasTokenForRange(const std::int32_t &fd, off_t start, off_t end, bool write_access)
    {
        std::lock_guard<std::mutex> lock(tokens_mutex);
        std::cout << "Inside HasTokenForRange " << std::endl;
        if (held_tokens.size() == 0)
        {
            std::cout << "held_tokens size is zero " << std::endl;
            return false;
        }

        for (const auto &token : held_tokens)
        {
            if (token.fd == fd &&
                token.start_offset <= start &&
                token.end_offset >= end &&
                (write_access == token.is_write))
            {
                return true;
            }
        }
        return false;
    }

    bool RequestToken(const std::int64_t &fd, off_t start, off_t end, bool write_access)
    {
        if (!stream)
        {
            std::cerr << "Stream is not initialized" << std::endl;
            return false;
        }

        pfs::StreamRequest request;

        request.set_client_id(this->client_id);
        request.set_file_descriptor(fd);
        request.set_start_offset(start);
        request.set_end_offset(end);
        request.set_is_write(write_access);

        std::cout << "Sending token request for client " << this->client_id
                  << " fd: " << fd
                  << " offset: " << start
                  << "-" << end
                  << " write: " << write_access << std::endl;

        bool write_result = stream->Write(request);
        if (!write_result)
        {
            std::cerr << "Failed to write token request to stream" << std::endl;
        }
        return write_result;
    }

private:
    void RemoveTokenRange(const std::string &filename, off_t start, off_t end)
    {
        auto it = held_tokens.begin();
        while (it != held_tokens.end())
        {
            if (it->filename == filename &&
                ((start <= it->start_offset && end >= it->start_offset) || // Overlap with start
                 (start <= it->end_offset && end >= it->end_offset)))      // Overlap with end
            {
                // Check if there is a left portion that is not conflicting
                if (start > it->start_offset)
                {
                    // Add the left non-conflicting portion
                    held_tokens.push_back({it->start_offset, start - 1, it->is_write, it->filename, it->fd});
                }

                // Check if there is a right portion that is not conflicting
                if (end < it->end_offset)
                {
                    // Add the right non-conflicting portion
                    held_tokens.push_back({end + 1, it->end_offset, it->is_write, it->filename, it->fd});
                }

                // Erase the original conflicting token
                it = held_tokens.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
        void AddTokenRange(const std::string &filename, off_t start, off_t end, bool is_write, std::int32_t fd)
        {
            TokenRange range{start, end, is_write, filename, fd};
            held_tokens.push_back(range);
        }
    };

std::unique_ptr<MetaServer::Stub> metaserver_stub;
std::vector<std::unique_ptr<FileServer::Stub>> fileserver_stubs;
std::unique_ptr<ClientTokenManager> token_manager;
std::int32_t client_id;

bool CheckServerAlive(std::int32_t id)
{
    pfs::AliveRequest request;
    pfs::AliveResponse response;
    ClientContext context;
    Status status = fileserver_stubs[id]->CheckAliveServer(&context, request, &response);

    return status.ok() && response.alive();
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
    std::cout << "Get my hostname -----------------";
    request.set_hostname("My host is Muni");

    pfs::RegisterClientResponse response;
    grpc::ClientContext context;

    Status status = metaserver_stub->RegisterClient(&context, request, &response);
    if (!status.ok() || !response.success())
    {
        std::cerr << "Failed to register client" << std::endl;
        return -1;
    }
    std::cout << "Got my hostname -----------------" << response.client_id() << std::endl;
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
    std::cout << "MetaServer Address is -----------------------------------------" << line << std::endl;
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
        std::cout << "FileServer Address is -----------------------------------------" << line << std::endl;
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
    std::cout << "Register Client is called---------------------------- " << std::endl;
    client_id = RegisterClient();
    // should we create a bidirectional stream while initialization only??

    token_manager = std::make_unique<ClientTokenManager>(client_id, std::move(token_manager_stub));
    return client_id;
}

int pfs_finish(int client_id)
{

    return 0;
}

int pfs_create(const char *filename, int stripe_width)
{
    std::cout << "Inside create file " << std::endl;

    if (!metaserver_stub)
    {
        std::cerr << "Metaserver stub is null" << std::endl;
        return -1;
    }

    pfs::CreateFileRequest request;
    request.set_filename(filename);
    request.set_stripe_width(stripe_width);

    pfs::CreateFileResponse response;
    grpc::ClientContext context;

    Status status = metaserver_stub->CreateFile(&context, request, &response);
    return status.ok() && response.success();
}

int pfs_open(const char *filename, int mode)
{

    return 0;
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset)
{
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    return 0;
}

int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset)
{

    std::cout << "Inside pfs_write " << std::endl;
    if (fd < 0 || buf == nullptr || num_bytes == 0)
    {
        std::cerr << "Invalid input parameters for pfs_write." << std::endl;
        return -1;
    }

    // Check/request token
    if (!token_manager->HasTokenForRange(fd, offset, offset + num_bytes, true))
    {
        if (!token_manager->RequestToken(fd, offset, offset + num_bytes, true))
        {
            return -1;
        }
    }
    return 0;
}

int pfs_close(int fd)
{

    return 0;
}

int pfs_delete(const char *filename)
{

    return 0;
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data)
{

    return 0;
}

int pfs_execstat(struct pfs_execstat *execstat_data)
{

    return 0;
}
