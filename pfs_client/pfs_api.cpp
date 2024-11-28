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

        TokenRange(off_t start, off_t end, bool write, const std::string& file, int file_descriptor)
        : start_offset(start), end_offset(end), is_write(write), filename(file), fd(file_descriptor) {}
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
        reader_thread = std::thread([this]()
                                    {
            pfs::StreamResponse response;
            while (stream->Read(&response)) {
                std::cout<<"Got response from the server to the client with the action "<<response.action()<<std::endl;
                std::lock_guard<std::mutex> lock(tokens_mutex);
                
                if (response.action() == "invalidate") {
                     std::cout << "Client should revoke the token from : " << response.start_offset() <<" to "<<response.end_offset()<< std::endl<< std::endl;
                    RemoveTokenRange(response.filename(), 
                                   response.start_offset(), 
                                   response.end_offset());
                      std::cout << "Client revoked the token from : " << response.start_offset()<<" to "<<response.end_offset()<<" and now sending the ack to the server."<< std::endl;
                    // Send ack
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

    bool HasTokenForRange(const std::int32_t &fd, off_t start, off_t end, bool write_access)
    {   std::cout<<"Checking if the token is held by the client"<<std::endl;
        std::lock_guard<std::mutex> lock(tokens_mutex);
        // std::cout << "Inside HasTokenForRange " << std::endl;
        if (held_tokens.size() == 0)
        {
            // std::cout << "held_tokens size is zero " << std::endl;
            std::cout<<"Client isn't holding the token so it requests for the token now "<<std::endl;
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
        std::cout<<"Client isn't holding the token so it requests for the token now "<<std::endl;
        return false;
    }

private:
    void RemoveTokenRange(const std::string &filename, off_t start, off_t end)
    {
        auto it = held_tokens.begin();
        std::vector<TokenRange> new_tokens;
        while (it != held_tokens.end())
        {   
            std::cout<<"printing it->start_offset "<<it->start_offset<<" and the start "<<start<<std::endl;
            std::cout<<"printing it->end_offset "<<it->end_offset<<" and the end "<<end<<std::endl;
            if (it->filename == filename &&
                ((start <= it->start_offset && end >= it->start_offset) || // Overlap with start
                 (start <= it->end_offset && end >= it->end_offset)))      // Overlap with end
            {
                // Check if there is a left portion that is not conflicting
                if (start > it->start_offset)
                {
                    // Add the left non-conflicting portion
                    std::cout << "Shrink of the token happened on the left side on the CLIENT SIDE " << it->start_offset << " to " << start - 1 << " with the token " << it->is_write << std::endl;
                    new_tokens.push_back(TokenRange(it->start_offset, start - 1, it->is_write, it->filename, it->fd));
                }

                // Check if there is a right portion that is not conflicting
                if (end < it->end_offset)
                {
                    // Add the right non-conflicting portion
                    std::cout << "Shrink of the token happened on the right side on the CLIENT SIDE " << end + 1 << " to " << it->end_offset << " with the token " << it->is_write << std::endl;
                    new_tokens.push_back(TokenRange(end + 1, it->end_offset, it->is_write, it->filename, it->fd));
                }

                // Erase the original conflicting token
                it = held_tokens.erase(it);
            }
            else{
                ++it;
            }
            // ++it;
        }
        held_tokens.insert(held_tokens.end(), new_tokens.begin(), new_tokens.end());
        std::cout << "Client 2 completed the process on the client side" << start << " to " << end << std::endl;
    }
    void AddTokenRange(const std::string &filename, off_t start, off_t end, bool is_write, std::int32_t fd)
    {
        std::cout << "Token added to the range " << start << " to " << end << std::endl;
        held_tokens.push_back(TokenRange(start, end, is_write, filename, fd));
        
        std::cout << "Client 1 completed the process on the client side" << start << " to " << end << std::endl;
    }
};

std::unique_ptr<MetaServer::Stub> metaserver_stub;
std::vector<std::unique_ptr<FileServer::Stub>> fileserver_stubs;
std::unique_ptr<ClientTokenManager> token_manager;
std::int32_t client_id;
// Fileserver Data Structures
std::unordered_map<std::string, int> fileStripeWidths;
std::unordered_map<int,std::string> fd_to_filename;
std::unordered_map<std::string,int> filename_to_fd;
std::unordered_map<int,int> file_mode;


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
    return status.ok() && response.success();
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

    token_manager = std::make_unique<ClientTokenManager>(client_id, std::move(token_manager_stub));
    return client_id;
}


int pfs_finish(int client_id)
{

    return 0;
}

int pfs_create(const char *filename, int stripe_width)
{
    std::cout<<"Inside create file "<<std::endl;  
    //Have to check for duplicate filename, if metaserver stub is null, if stripe width is greater than the no of file servers

  //Have to check for duplicate filename
  for (const auto &pair : fileStripeWidths) {
        if (pair.first == filename) {
            std::cerr << "Duplicate filename: " << filename << std::endl;
            return -1;
        }
    }


    if (!metaserver_stub) {
        std::cerr << "Metaserver stub is null" << std::endl;
        return -1;
    }

    if (stripe_width > fileserver_stubs.size()) {
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
    if (!meta_status.ok() || !meta_response.success()) {
        std::cerr << "Failed to create metadata on metaserver: " << meta_status.error_message() << std::endl;
        return -1;
    }
    std::cout << "Metadata for file " << filename << " created successfully." << std::endl;
    //int fd = meta_response.file_descriptor();
    //No fileserver handling as the there is no data to be written into the fileservers
    // int fd = meta_response.file_descriptor();
    // fileStripeWidths[fd] = stripe_width;
    // fd_to_filename[fd] = filename;
    // No file descriptor is assigned here
    fileStripeWidths[filename] = stripe_width;
    return 0;
}

int pfs_open(const char *filename, int mode)
{

     // Validate input
    if (filename == nullptr || strlen(filename) == 0) {
        std::cerr << "Invalid filename for pfs_open." << std::endl;
        return -1;
    }

    if (mode != 1 && mode != 2) {
        std::cerr << "Invalid mode for pfs_open. Expected 1 (read) or 2 (read/write)." << std::endl;
        return -1;
    }

    // Check if file is already open
    for (const auto &pair : fd_to_filename) {
        if (pair.second == filename) {
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
    std::cout<<"The status is ok : "<<status.ok()<<std::endl;
    std::cout<<"The response success is : "<<response.success()<<std::endl;
    if (!status.ok() || !response.success()) {
        std::cerr << "Failed to open file on metadata server: " << status.error_message() << std::endl;
        return -1;
    }

    // Step 3: Extract the file descriptor and other metadata
    int fd = response.file_descriptor();
    // int stripe_width = fileStripeWidths[fd];

    // Step 4: Update local structures for file descriptor and metadata management
    // fileStripeWidths[fd] = stripe_width;
    fd_to_filename[fd] = filename;
    filename_to_fd[filename]=fd;
    file_mode[fd]=mode;

    std::cout << "File opened successfully with FD: " << fd <<" in mode: " << mode<< std::endl;
    
    return fd;
}
int pfs_doTheRead(int fd, void *buf, size_t num_bytes, off_t offset){
    // ...

    // Check client cache
    // cache_func_temp();

    // ...
    

    // Retrieve metadata for the file
    std::string filename = fd_to_filename[fd];
    std::cout<<"The file being read is : "<<filename<<std::endl;
    //int stripe_width = fileStripeWidths[filename];
    //int stripe_width = 1;

    // if (stripe_width <= 0 || stripe_width > fileserver_stubs.size()) {
    //     std::cerr << "Invalid stripe width: " << stripe_width << std::endl;
    //     return -1;
    // }

    // Step 1: Fetch file size from metadata server
    pfs::FetchMetadataRequest metadata_request;
    metadata_request.set_file_descriptor(fd);

    pfs::FetchMetadataResponse metadata_response;
    grpc::ClientContext metadata_context;

    Status metadata_status = metaserver_stub->FetchMetadata(&metadata_context, metadata_request, &metadata_response);
    if (!metadata_status.ok() || !metadata_response.success()) {
        std::cerr << "Failed to fetch metadata of the file " << metadata_status.error_message() << std::endl;
        return -1;
    }

    off_t file_size = metadata_response.file_size();
    if (offset >= file_size) {
        // Offset beyond end-of-file
        std::cout<<" File size is less than that of the offset"<<std::endl;
        return 0;
    }

    int stripe_width = metadata_response.stripe_width();
    if (stripe_width <= 0 || stripe_width > fileserver_stubs.size()) {
        std::cerr << "Invalid stripe width: " << stripe_width << std::endl;
        return -1;
    }

    // Adjust num_bytes if reading beyond the end of the file
    num_bytes = std::min(static_cast<off_t>(num_bytes), file_size - offset);

    size_t remaining_bytes = num_bytes;
    char *buffer_ptr = static_cast<char *>(buf);

    std::mutex buffer_mutex;
    std::vector<std::thread> threads;

    // Step 2: Read data from the file in a striped manner
    while (remaining_bytes > 0) {
        // Determine stripe and server index
        int stripe_index = offset / (PFS_BLOCK_SIZE * STRIPE_BLOCKS);
        int server_index = stripe_index % stripe_width;
        int block_offset = offset % (PFS_BLOCK_SIZE * STRIPE_BLOCKS);

        size_t pfs_block_size = PFS_BLOCK_SIZE;

        size_t bytes_to_read = std::min(remaining_bytes, pfs_block_size - (offset % pfs_block_size));

        // Generate block name
        std::string block_name = filename + "." + std::to_string(server_index);

         size_t file_stripe_offset = block_offset + (stripe_index / stripe_width) * PFS_BLOCK_SIZE * STRIPE_BLOCKS;

        // Create a thread for each read task
        auto read_task = [&](size_t bytes_to_read, size_t file_stripe_offset, char *buffer_section, int server_index, std::string block_name)
        {
            pfs::ReadChunkRequest fs_request;
            fs_request.set_filename(block_name);
            fs_request.set_offset(file_stripe_offset);
            fs_request.set_num_bytes(bytes_to_read);

            pfs::ReadChunkResponse fs_response;
            grpc::ClientContext fs_context;

            // Perform the read operation
            Status fs_status = fileserver_stubs[server_index]->ReadChunk(&fs_context, fs_request, &fs_response);
            if (!fs_status.ok() || !fs_response.success())
            {
                std::cerr << "Failed to read data from fileserver " << server_index
                          << ": " << fs_status.error_message() << std::endl;
                return;
            }

            // Write the chunk data to the buffer
            std::lock_guard<std::mutex> lock(buffer_mutex);
            std::string chunk_data = fs_response.data();
            memcpy(buffer_section, chunk_data.data(), chunk_data.size());

            std::cout << "Successfully read " << chunk_data.size()
                      << " bytes from fileserver " << server_index
                      << " at offset " << file_stripe_offset
                      << " from file chunk " << block_name << std::endl;
        };

        threads.emplace_back(read_task, bytes_to_read, file_stripe_offset, buffer_ptr, server_index, block_name);

       

        // Update counters and pointers
        remaining_bytes -= bytes_to_read;
        buffer_ptr += bytes_to_read;
        offset += bytes_to_read;
    }

    // Join all threads
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    std::cout<<"Num of bytes: "<<num_bytes<<" Remaining bytes: "<<remaining_bytes<<std::endl;

    // Return the actual number of bytes read
    return num_bytes - remaining_bytes;

    
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset)
{
    // ...
    std::cout << "Inside pfs_read " << std::endl;
    //Validate input parameters
    if (fd < 0 || buf == nullptr || num_bytes == 0) {
        std::cerr << "Invalid input parameters for pfs_read." << std::endl;
        return -1;
    }

    if(file_mode[fd] != 1){
        std::cerr << "The mode is not 'read' for file descriptor " << fd << std::endl;
        return -1;
    }

    //Check if file descriptor exists
    if (fd_to_filename.find(fd) == fd_to_filename.end()) {
        std::cerr << "Filename not found for file descriptor: " << fd << std::endl;
        return -1;
    }
    //Token Management for Read
    if(token_manager->HasTokenForRange(fd, offset, offset+num_bytes, 0)){
        std::cout<<"Reading to file started in the HasToken"<<std::endl;
        pfs_doTheRead(fd, buf, num_bytes, offset);
    }
    // Check/request token
    else if(RequestToken(fd, offset, offset+num_bytes, 0)){
        std::cout<<"Reading to file started in the Request Token"<<std::endl;
        pfs_doTheRead(fd, buf, num_bytes, offset);
    }

    // Check client cache
    // cache_func_temp();

    // ...

    return 0;
}
int pfs_doTheWrite(int fd, const void *buf, size_t num_bytes, off_t offset){
        // ...

    // Check client cache
    // cache_func_temp();

    // ...

    std::cout<<"Inside pfs_doTheWrite with threading"<<std::endl;

    std::string filename = fd_to_filename[fd];

    pfs::FetchMetadataRequest metadata_request;
    metadata_request.set_file_descriptor(fd);

    pfs::FetchMetadataResponse metadata_response;
    grpc::ClientContext metadata_context;

    Status metadata_status = metaserver_stub->FetchMetadata(&metadata_context, metadata_request, &metadata_response);
    if (!metadata_status.ok() || !metadata_response.success()) {
        std::cerr << "Failed to fetch metadata of the file " << metadata_status.error_message() << std::endl;
        return -1;
    }

    off_t current_file_size = metadata_response.file_size();
    if (offset > current_file_size) {
        // Offset beyond end-of-file
        std::cout<<"The current size of the file is: "<<current_file_size<<std::endl;
        std::cout<<"The offset given to write is :"<<offset<<std::endl;
        std::cout<<" File size is less than that of the offset"<<std::endl;
        return 0;
    }

    //std::string filename = metadata_response.filename;
    int stripe_width = metadata_response.stripe_width();
    
    size_t remaining_bytes = num_bytes;
    const char *data_ptr = static_cast<const char *>(buf);

    std::mutex write_mutex;
    std::vector<std::thread> threads;

    while (remaining_bytes > 0) {
        // Determine the block and server for the current offset
        int stripe_index = offset / (PFS_BLOCK_SIZE * STRIPE_BLOCKS);
        std::cout<<"The stripe width is : "<<stripe_width<<std::endl;
        int server_index = stripe_index % stripe_width;
        int block_offset = offset % (PFS_BLOCK_SIZE * STRIPE_BLOCKS);

        size_t pfs_block_size = PFS_BLOCK_SIZE;

        size_t bytes_to_write = std::min(remaining_bytes, pfs_block_size - (offset % pfs_block_size));

        std::string block_name = fd_to_filename[fd] + "." + std::to_string(server_index);

        size_t file_stripe_offset = block_offset + (stripe_index / stripe_width) * PFS_BLOCK_SIZE * STRIPE_BLOCKS;

        std::cout << "Stripe Index: " << stripe_index 
          << ", Server Index: " << server_index 
          << ", Block Offset: " << block_offset 
          << ", Bytes to Write: " << bytes_to_write 
          << ", File Stripe Offset: "<<file_stripe_offset
          << ", Block Name: " << block_name 
          << ", Remaining Bytes: " << remaining_bytes 
          << ", Current Offset: " << offset 
          << ", Data Pointer Address: " << static_cast<const void *>(data_ptr) 
          << std::endl;

         // Lambda function for writing data
        auto write_task = [&](std::string block_name, size_t file_stripe_offset, const char *data, size_t length, int server_index)
        {
            pfs::WriteChunkRequest fs_request;
            fs_request.set_filename(block_name);
            fs_request.set_offset(file_stripe_offset);
            fs_request.set_data(std::string(data, length));

            pfs::WriteChunkResponse fs_response;
            grpc::ClientContext fs_context;

            Status fs_status = fileserver_stubs[server_index]->WriteChunk(&fs_context, fs_request, &fs_response);
            if (!fs_status.ok() || !fs_response.success())
            {
                std::lock_guard<std::mutex> lock(write_mutex);
                std::cerr << "Failed to write data to fileserver " << server_index
                          << ": " << fs_status.error_message() << std::endl;
                return;
            }

            std::lock_guard<std::mutex> lock(write_mutex);
            std::cout << "Successfully wrote " << fs_response.bytes_written()
                      << " bytes to fileserver " << server_index
                      << " at offset " << file_stripe_offset << std::endl;
        };

        // Launch a thread for the current write operation
        threads.emplace_back(write_task, block_name, file_stripe_offset, data_ptr, bytes_to_write, server_index);

        // Update pointers and counters
        remaining_bytes -= bytes_to_write;
        data_ptr += bytes_to_write;
        offset += bytes_to_write;
    }
    // Join all threads
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    size_t new_end_position = offset + num_bytes;
     if (new_end_position > current_file_size) {
        // Update the file size in the metadata server
        pfs::UpdateMetadataRequest update_request;
        update_request.set_file_descriptor(fd);
        update_request.set_new_file_size(new_end_position);

        pfs::UpdateMetadataResponse update_response;
        grpc::ClientContext context;

        Status status = metaserver_stub->UpdateMetadata(&context, update_request, &update_response);
        if (!status.ok() || !update_response.success()) {
            std::cerr << "Failed to update metadata: " << status.error_message() << std::endl;
            return -1;
        }

     }
    return num_bytes;
}
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {

    std::cout << "Inside pfs_write " << std::endl;
    //TODO: Whether to put the edge case conditions here or in the pfs_doTheWrite 
    if (fd < 0 || buf == nullptr || num_bytes == 0)
    {
        std::cerr << "Invalid input parameters for pfs_write." << std::endl;
        return -1;
    }

    if(file_mode[fd] != 2){
        std::cerr << "The mode is not 'write' for file descriptor " << fd << std::endl;
        return -1;
    }

    //Check if file descriptor exists
    if (fd_to_filename.find(fd) == fd_to_filename.end()) {
        std::cerr << "Filename not found for file descriptor: " << fd << std::endl;
        return -1;
    }

    //Token Management for Write
    if(token_manager->HasTokenForRange(fd, offset, offset+num_bytes, 1)){
        std::cout<<"Writing to file started in the HasToken"<<std::endl;
        pfs_doTheWrite(fd, buf, num_bytes, offset);
    }
    // Check/request token
    else if(RequestToken(fd, offset, offset+num_bytes, 1)){
        std::cout<<"Writing to file started in the Request Token"<<std::endl;
        pfs_doTheWrite(fd, buf, num_bytes, offset);
    }
    else{
        std::cerr << "Neither the Request to the token is granted nor the client has the privilage" << std::endl;
        return -1;
    }
   
    return 0;
}

int pfs_printAllTokensFromServer() {
    pfs::GetAllTokensRequest request;
    pfs::GetAllTokensResponse response;
    grpc::ClientContext context;
    
    Status status = metaserver_stub->GetAllTokens(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "Failed to get tokens from MetaServer: " << status.error_message() << std::endl;
        return -1;
    }

    std::cout << "Tokens held by all clients:" << std::endl;
    for (const auto& token_info : response.tokens()) {
        std::cout << "Client ID: " << token_info.client_id()
                  << ", Range: [" << token_info.start_offset() << ", " << token_info.end_offset() << "]"
                  << ", Type: " << (token_info.is_write() ? "Write" : "Read")
                  << std::endl;
    }
    return 0;
}

int pfs_close(int fd)
{
    // Step 1: Validate file descriptor
    if (fd < 0) {
        std::cerr << "Invalid file descriptor for pfs_close: " << fd << std::endl;
        return -1;
    }

    // Check if the file descriptor exists
    auto it = fd_to_filename.find(fd);
    if (it == fd_to_filename.end()) {
        std::cerr << "File descriptor not found or file is not open: " << fd << std::endl;
        return -1;
    }
    
    std::string filename = it->second;

    // Step 2: Notify the metadata server 
    pfs::CloseFileRequest request;
    request.set_file_descriptor(fd);

    pfs::CloseFileResponse response;
    grpc::ClientContext context;

    Status status = metaserver_stub->CloseFile(&context, request, &response);
    if (!status.ok() || !response.success()) {
        std::cerr << "Failed to close file descriptor on metadata server: " << status.error_message() << std::endl;
        return -1;
    }

    std::cout << "File descriptor " << fd << " successfully closed on metadata server." << std::endl;

    // Step 3: Remove file descriptor from local tracking structures
    fd_to_filename.erase(fd);
    file_mode.erase(fd);

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
