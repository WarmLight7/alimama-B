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
using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;


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


class SearchServiceImpl final : public SearchService::Service , public Sort::SortMethod{
private:
    std::unique_ptr<SearchService::Stub> stub_node[2];
    bool isAvailable = false;
    int hostNode;
    //异步GRPC
    std::mutex asyncmutex[2];
    std::condition_variable asynccv[2];
    CompletionQueue cq_[2];//异步GRPC
    std::unique_ptr<std::vector<Response>> async_grpc_node[2]; // reqid, res
    std::unique_ptr<bool[]> async_grpc_ready[2]; 
    std::atomic<uint64_t> reqid{0};
    //异步GRPC
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        Response reply;
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;
        // Storage for the status of the RPC upon completion.
        Status status;
        std::unique_ptr<ClientAsyncResponseReader<Response>> response_reader;
    };

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
        //异步GRPC
        for(int i = 0 ; i < 2 ; i++){
            async_grpc_node[i] = std::make_unique<std::vector<Response>>(100001); // 十万数量级
            async_grpc_ready[i] = std::make_unique<bool[]>(100001);
            for(int j = 0; j < 100001; j++){
                async_grpc_ready[i][j] = false;
            }
            std::thread AsyncRPCthread(&SearchServiceImpl::AsyncCompleteRpc, this, i);
            AsyncRPCthread.detach();
        }
        isAvailable = true;
        
    }

    //异步GRPC
    // 异步监听函数
    void AsyncCompleteRpc(int nodeID) {
        void* got_tag;
        bool ok = false;
        // Block until the next result is available in the completion queue "cq".
        while (cq_[nodeID].Next(&got_tag, &ok)) {
            // The tag in this example is the memory location of the call object
            AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            GPR_ASSERT(ok);

            if (call->status.ok()){
                int curindex = call->reply.resid();
                (*async_grpc_node[nodeID])[curindex] = call->reply;
                async_grpc_ready[nodeID][curindex] = true;
                std::unique_lock<std::mutex> lock(asyncmutex[nodeID]);
                asynccv[nodeID].notify_all();
                lock.unlock();
            }
            else
                std::cout << "RPC0 failed" << std::endl;

            // Once we're complete, deallocate the call object.
            delete call;
        }
    }

    Status InnerSearch(ServerContext* context, const Request* request, Response* response) override {
        //std::cout << "开始了一次Innersearch" <<  request->reqid() << std::endl;
        getListHeapsort(request, response);
        //std::cout << "结束了一次Innersearch" <<  request->reqid() << std::endl;
        response->set_resid(request->reqid());//异步GRPC
        return Status::OK;
    }

    Status Search(ServerContext* context, const Request* request, Response* response) override {
        uint32_t topn = request->topn();
        Request* mutableRequest = const_cast<Request*>(request);//异步GRPC
//异步GRPC
        uint64_t curReqid = reqid.fetch_add(1)%100000;
        int targetNode = curReqid%3;
        mutableRequest->set_reqid(curReqid);
        if(targetNode == 2){
            InnerSearch(context, mutableRequest, response);
            return Status::OK;
        }   
        AsyncClientCall* call = new AsyncClientCall;
        call->response_reader = stub_node[targetNode]->PrepareAsyncInnerSearch(&call->context, *mutableRequest, &cq_[targetNode]);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        //异步GRPC
        std::unique_lock<std::mutex> lock(asyncmutex[targetNode]);
        //std::cout << "开始了一次访问" <<  curReqid  << std::endl;
        while(async_grpc_ready[targetNode][curReqid] == false){
            asynccv[targetNode].wait(lock);
        }
        
        *response = (*async_grpc_node[targetNode])[curReqid];
        //std::cout << "结束了一次访问" <<  response->resid() <<std::endl;
        Response newResponse;
        (*async_grpc_node[targetNode])[curReqid] = newResponse;
        async_grpc_ready[targetNode][curReqid] = false;
        lock.unlock();
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
//     SearchServiceImpl server;
//     server.Run();
    return 0;
}
