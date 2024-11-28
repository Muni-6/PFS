#include "pfs_fileserver.hpp"
#include <grpcpp/grpcpp.h>
#include "../pfs_proto/pfs_fileserver.grpc.pb.h"
#include <filesystem> // Include for filesystem utilities


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfs;
class  FileServerImpl final:public FileServer :: Service{
    private:
   
    //methods here
    Status CheckAliveServer(ServerContext* context, const AliveRequest* request,
                      AliveResponse* response) override {
        std::cout << "Received Alive check request." << std::endl;
        response->set_alive(true);
        response->set_server_message("File server is healthy.");
        return Status::OK;
    }

    Status WriteChunk(ServerContext* context, const WriteChunkRequest* request, WriteChunkResponse* response) {
        const std::string& filename = request->filename();
        int64_t offset = request->offset();
        const std::string& data = request->data();

        std::cout << "Write Chunk:   =====================    "<<"Filename: " << filename 
          << ", Offset: " << offset 
          << ", Data: " << data 
          << std::endl;



        // Attempt to open the file for reading and writing
        std::fstream file;
        file.open(filename, std::ios::in | std::ios::out | std::ios::binary);

        // If the file cannot be opened, attempt to create it
        if (!file.is_open()) {
            file.open(filename, std::ios::out | std::ios::binary); // Create the file
            if (!file.is_open()) {
                response->set_success(false);
                response->set_error_message("Failed to create or open the file.");
                return Status::CANCELLED;
            }
            file.close(); // Close after creating to reopen in the desired mode
            file.open(filename, std::ios::in | std::ios::out | std::ios::binary);
        }

        // If the file cannot be opened, return an error
        if (!file.is_open()) {
            response->set_success(false);
            response->set_error_message("File does not exist or cannot be opened for writing.");
            return Status::CANCELLED;
        }

        // Retrieve the absolute path
        std::string absolute_path;
        try {
            absolute_path = std::filesystem::absolute(filename).string();
            std::cout << "File absolute path: " << absolute_path << std::endl;
        } catch (const std::filesystem::filesystem_error& e) {
            std::cout<<"Failed to retrieve absolute file path: " + std::string(e.what());
        }

        // Seek to the correct position and write data
        file.seekp(offset, std::ios::beg);
        file.write(data.data(), data.size());
        file.close();

        // Populate the response
        response->set_success(true);
        response->set_bytes_written(data.size());
        return Status::OK;
    }


    Status ReadChunk(ServerContext* context, const ReadChunkRequest* request, ReadChunkResponse* response) {
        const std::string& filename = request->filename();
        int64_t offset = request->offset();
        int64_t num_bytes = request->num_bytes();

        std::cout << "Read Chunk:   =====================    "<<"Filename: " << filename 
          << ", Offset: " << offset 
          << ", Num of bytes: " << num_bytes 
          << std::endl;

        // Open file for reading
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) {
            response->set_success(false);
            response->set_error_message("Unable to open file for reading.");
            return Status::CANCELLED;
        }

        // Seek to the correct position and read data
        file.seekg(offset, std::ios::beg);
        std::vector<char> buffer(num_bytes);
        file.read(buffer.data(), num_bytes);
        file.close();

        // Populate the response
        response->set_success(true);
        response->set_data(std::string(buffer.begin(), buffer.begin() + file.gcount()));
        return Status::OK;
    }
    Status DeleteChunk(ServerContext* context, const DeleteChunkRequest* request, DeleteChunkResponse* response) {
        const std::string& filename = request->filename();

        // Attempt to delete the file
        if (std::remove(filename.c_str()) != 0) {
            response->set_success(false);
            response->set_error_message("Failed to delete file.");
            return Status::CANCELLED;
        }

        response->set_success(true);
        return Status::OK;
    }

    Status GetChunkInfo(ServerContext* context, const GetChunkInfoRequest* request, GetChunkInfoResponse* response) {
        const std::string& filename = request->filename();

        // Get file size
        std::ifstream file(filename, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            response->set_success(false);
            response->set_error_message("Unable to open file.");
            return Status::CANCELLED;
        }

        response->set_file_size(file.tellg());
        response->set_success(true);
        return Status::OK;
    }



 

};
void RunFileServer(const std::string& server_address){
  FileServerImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}
int main(int argc, char *argv[]) {
    printf("%s:%s: PFS file server start! Hostname: %s, IP: %s\n",
           __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    bool found = false;
    std::string line;
    std::getline(pfs_list, line); // First line is the meta server
    while (std::getline(pfs_list, line)) {
        if (line.substr(0, line.find(':')) == getMyHostname()) {
            found = true;
            break;
        }
    }
    if (!found) {
        fprintf(stderr, "%s: hostname not found in pfs_list.txt.\n", __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);

   
    RunFileServer("0.0.0.0:"+listen_port);
    // Run the PFS fileserver and listen to requests
    printf("%s: Launching PFS file server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...
    

    
    printf("%s:%s: PFS file server done!\n", __FILE__, __func__);
    return 0;
}