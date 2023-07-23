#include <iostream>
#include <memory>
#include <string>
#include <cstdlib>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <etcd/Client.hpp>

#ifdef BAZEL_BUILD
#include "examples/protos/alimama.grpc.pb.h"
#else
#include "alimama.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ClientContext;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;
using alimama::proto::AvailabilityRequest;
using alimama::proto::AvailabilityResponse;

#include <sys/socket.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sort.h>


std::string getLocalIP() {
    struct ifaddrs *ifAddrStruct = NULL;
    void *tmpAddrPtr = NULL;
    std::string localIP;
    getifaddrs(&ifAddrStruct);
    while (ifAddrStruct != NULL) {
        if (ifAddrStruct->ifa_addr->sa_family == AF_INET) {
            tmpAddrPtr = &((struct sockaddr_in *)ifAddrStruct->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            std::string interfaceName(ifAddrStruct->ifa_name);
            if (interfaceName == "en0" || interfaceName == "eth0") {
                return addressBuffer;
            }
        }
        ifAddrStruct = ifAddrStruct->ifa_next;
    }
    return "";
}


class SearchServiceImpl final : public SearchService::Service , public SortMethod{
private:
    bool isAvailable = false;
    std::unique_ptr<SearchService::Stub> stub_node[2];
    int hostNode;
    std::atomic<uint64_t> searchid{0};
public:
    SearchServiceImpl(){
        char *hostname = std::getenv("NODE_ID");
        hostNode = std::stoi(hostname);
        int index = 0;
        if(hostNode == 1){
            for (int i = 0; i < 2; i++){
                std::string distNode = "node-" + std::to_string(i+2) + ":50051";
                stub_node[i] = SearchService::NewStub(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
            }
        }
        readCsv("/data/raw_data.csv");
        isAvailable = true;
    }

    Status InnerSearch(ServerContext* context, const Request* request, Response* response) override {
        getListHeapsort(request, response);
        return Status::OK;
    }

    Status Search(ServerContext* context, const Request* request, Response* response) override {
        int subnode_id = searchid.fetch_add(1)%3;
        if(subnode_id == 2){
            InnerSearch(context, request, response);
        }
        else{
            grpc::ClientContext client_context;
            stub_node[subnode_id]->InnerSearch(&client_context, *request, response);
        }
        

        return Status::OK;
    }

    Status CheckAvailability(ServerContext* context, const AvailabilityRequest* request, AvailabilityResponse* response) override {
        // 根据可用性设置响应字段
        if (isAvailable) {
            response->set_available(true);
        } else {
            response->set_available(false);
        }
        return Status::OK;
    }

    void WaitService() {
        AvailabilityRequest request[2];
        AvailabilityResponse response[2];
        ClientContext context[2];
        for(int i = 0 ; i < 2 ; i++){
            Status status = stub_node[i]->CheckAvailability(&context[i], request[i], &response[i]);
            while(!status.ok() || !response[i].available()){
                ClientContext tempContext;
                status = stub_node[i]->CheckAvailability(&tempContext, request[i], &response[i]);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }
};


void RunServer() {
    std::string env_var_str = std::string("0.0.0.0");
    std::string local_ip = getLocalIP();
    std::cout << "local_ip " << local_ip << std::endl;

    constexpr int kPort = 50051;
    std::string external_address = local_ip + std::string(":") + std::to_string(kPort);
    std::string server_address(std::string("0.0.0.0:") + std::to_string(kPort));
    std::string key = std::string("/services/searchservice");


    SearchServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    char *hostname = std::getenv("NODE_ID");
    int hostNode = std::stoi(hostname);
    std::cout << "hostNode:" << hostNode << std::endl;
    std::cout << "external_address " << external_address << std::endl;
    etcd::Client etcd("http://etcd:2379");
    if(hostNode == 1){
        //创建一个etcd客户端
        service.WaitService();
        
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        // 将服务地址注册到etcd中
        auto response = etcd.set(key, external_address).get();
        if (response.is_ok()) {
            std::cout << "Service registration successful.\n";
        } else {
            std::cerr << "Service registration failed: " << response.error_message() << "\n";
        }

        std::cout << "Server listening on " << server_address  << std::endl;
        
    }

    server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
