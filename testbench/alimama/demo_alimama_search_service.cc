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
using grpc::ClientContext;
using grpc::Status;
using grpc::ServerCompletionQueue;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;
using alimama::proto::InnerResponse;
using alimama::proto::Greeter;
using alimama::proto::AdgroupMessage;


#include <sys/socket.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <cmath>
#include <vector>
#include <bits/stdc++.h>
#include "csv.h"

std::vector<uint64_t> split2int(const std::string& str, char delimiter) {
    std::vector<uint64_t> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(std::stoull(token));
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

int main(int argc, char** argv) {

    //node-1监听
    // RunServer();
    //node-2访问
    std::string diskCache = "node-1:50051";
    std::unique_ptr<SearchService::Stub> stub_(SearchService::NewStub(grpc::CreateChannel(diskCache, grpc::InsecureChannelCredentials())));
    std::string csvFile = "/data/test_case_dev.csv";
    csv::CSVReader<6, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
    std::string keywords;
    std::string uservectorString;
    uint64_t hour;
    uint64_t topn;
    std::string adgroup_idstring;
    std::string pricestring;
    int currentRow = 0;
    std::cout << currentRow << "currentRow";
    while (currentRow < 1 && reader.read_row(keywords,uservectorString,hour,topn,adgroup_idstring,pricestring)){
        Request request;
        std::vector<uint64_t> keywordList = split2int(keywords, ',');
        std::vector<uint64_t> adgroup_id = split2int(adgroup_idstring, ',');
        std::vector<uint64_t> prices = split2int(pricestring, ',');
        for(int i = 0 ; i < keywordList.size() ; i++){
            request.add_keywords(keywordList[i]);
        }
        std::pair<float, float> uservector = split2float(uservectorString);
        request.add_context_vector(uservector.first);
        request.add_context_vector(uservector.second);
        request.set_hour(hour);
        request.set_topn(topn);
        
        std::cout << "adgroup_id:";
        for(int i = 0 ; i < adgroup_id.size() ; i++){
            std::cout << adgroup_id[i] << " ";
        }
        std::cout << std::endl;
        Response reply;
        grpc::ClientContext context;
        std::cout << "正在读取..."<< std::endl;
        Status status = stub_->Search(&context, request, &reply);
        int lenth = reply.adgroup_ids().size();
        std::cout << "adgroup_id:";
        for(int i = 0 ; i < lenth ; i++){
            std::cout << reply.adgroup_ids()[i] << " ";
        }
        std::cout << std::endl;
        
        

        std::cout << "price:";
        for(int i = 0 ; i < lenth ; i++){
            std::cout << reply.prices()[i] << " ";
        }
        std::cout << std::endl;

    }
    

    // 应响应输出644960096148,1710671559561	27435,39778
  return 0;
}