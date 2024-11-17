#include "pfs_fileserver.hpp"
#include <grpcpp/grpcpp.h>
#include "../pfs_proto/pfs_fileserver.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfs;
class  FileServerImpl final:public FileServer :: Service{
    //methods here
    Status CheckAliveServer(ServerContext* context, const AliveRequest* request,
                      AliveResponse* response) override {
        std::cout << "Received Alive check request." << std::endl;
        response->set_alive(true);
        response->set_server_message("File server is healthy.");
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
