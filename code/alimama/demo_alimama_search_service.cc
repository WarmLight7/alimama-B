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
using grpc::ServerCompletionQueue;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;


#include <sys/socket.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <cmath>
#include <vector>
#include <bits/stdc++.h>

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

class SearchServiceImpl final : public SearchService::Service {
private:
    std::map<int, int> keywordID;
    std::map<int, int> adgroupID;
    std::vector<std::set<int>> keywordAdgroupSet;
    std::vector<std::map<int, pair<float, float>>> keywordAdgroup2vector; 
    std::map<int, int> adgroup2price;
    std::map<int, int> adgroup2timings;  //使用2^24次存储 用int就够 
public:
    
  
  //转换判断类型
    int timings2int(vector<int>& timings, int status){
        int timing = 0;
        for (int i = binaryArray.size()-1; i >= 0; i--) {
            timing = (result << 1) | !(timings[i]^status);
        }
        return timing;
    }
    int hours2int(int hour){
        return 1 << (hour-1);
    }
    bool checkHours(int timing, int hour){
        hour = 1 << (hour-1);
        return (timing & hour) != 0;
    }

    //csv读取
    std::vector<int> split2int(const std::string& str, char delimiter) {
        std::vector<int> tokens;
        std::stringstream ss(str);
        int token;
        while (std::getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }
    std::pair<float, float> split2float(const std::string& str) {
        std::pair<float, float> result;
        std::stringstream ss(str);
        ss >> result.first;
        ss.ignore(); // 忽略逗号或其他分隔符
        ss >> result.second;
        return result;
    }
    void processeline(const std::string& str, char delimiter) {
        
        std::stringstream ss(str);
        std::string token;
        // std::vector<std::string> tokens;
        // while (std::getline(ss, token, delimiter)) {
        //     tokens.push_back(token);
        // }

        uint64_t keyword;
        std::getline(ss, keyword, delimiter);
        if (keywordID.find(keyword) == keywordID.end()) {
            keywordID[keyword] = keywordID.size();
        }
        keyword = keywordID[keyword];

        uint64_t adgroup;
        std::getline(ss, adgroup, delimiter);
        if (adgroupID.find(adgroup) == adgroupID.end()) {
            adgroupID[adgroup] = adgroupID.size();
        }
        adgroup = adgroupID[adgroup];
        keywordAdgroupSet[keyword].insert(adgroup);

        uint32_t price;
        std::getline(ss, price, delimiter);
        adgroup2price[adgroup] = price;

        uint8_t status;
        std::getline(ss, status, delimiter);
        std::getline(ss, token, delimiter);
        std::vector<int> timings = split2int(token, ',');
        uint32_t timing = timings2int(timings, status);
        adgroup2timings[adgroup] = timing;

        std::getline(ss, token, delimiter);
        std::pair<float, float> itemVector = split2float(token);
        keywordAdgroup2vector[keyword][adgroup] = itemVector;

        std::getline(ss, token, delimiter);
        std::getline(ss, token, delimiter);
    }

    std::vector<std::vector<std::string>> read_csv_rows(const std::string& csv_file, int start_row, int end_row) {
        std::ifstream file(csv_file);
        std::string line;
        int row_num = 0;
        while (std::getline(file, line) && row_num < end_row) {
            if (row_num >= start_row) {
                processeline(line, '\t');
            }
            row_num++;
        }
        file.close();
    }
    
    void readCsv(const std::string& path){
        int start_row = 0;  // 起始行
        int end_row = 20;  
        read_csv_rows(path, start_row, end_row);
    }

    GreeterServiceImpl() {
        readCsv("/data/raw_data.csv");
    }

    Status Search(ServerContext* context, const Request* request, Response* response) override {
    // 作为示例，我们只是简单地返回一些假数据。
    // 假设我们找到了两个广告单元
    response->add_adgroup_ids(12345);
    response->add_adgroup_ids(67890);

    // 对应的价格
    response->add_prices(100);
    response->add_prices(200);
    return Status::OK;
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

    // 创建一个etcd客户端
    etcd::Client etcd("http://etcd:2379");

    // 将服务地址注册到etcd中
    auto response = etcd.set(key, external_address).get();
    if (response.is_ok()) {
        std::cout << "Service registration successful.\n";
    } else {
        std::cerr << "Service registration failed: " << response.error_message() << "\n";
    }

    std::cout << "Server listening on " << server_address  << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
