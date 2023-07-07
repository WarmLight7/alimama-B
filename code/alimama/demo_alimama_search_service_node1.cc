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
#include "csv.h"

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

struct AdGroup {
    float score;
    float price;
    float ctr;
    uint64_t adgroup_id;
    bool operator==(const AdGroup& other) const{
        return adgroup_id == other.adgroup_id;
    }
    bool operator<(const AdGroup& other) const {
    if (std::abs(score - other.score) > 1e-6) {
        return score > other.score;
    }

    if (std::abs(price - other.price) > 1e-6) {
        return price < other.price;
    }

    return adgroup_id > other.adgroup_id;
}
};


class SearchServiceImpl final : public SearchService::Service {
private:
    std::map<uint64_t, uint32_t> keywordID;
    std::map<uint64_t, uint32_t> adgroupID;
    std::map<uint32_t, uint64_t> ID2adgroup;
    std::vector<std::set<uint32_t>> keywordAdgroupSet;
    std::vector<std::map<uint32_t, std::pair<float, float> > > keywordAdgroup2vector; 
    std::map<uint32_t, uint32_t> adgroup2price;
    std::map<uint32_t, std::bitset<24> > adgroup2timings;  //使用2^24次存储 用int就够 

    grpc::ClientContext context2;
    std::unique_ptr<SearchService::Stub> stub_node2;
    grpc::ClientContext context3;
    std::unique_ptr<SearchService::Stub> stub_node3;

public:
      
  //转换判断类型
    std::bitset<24> timings2bitset(std::string& timings, uint8_t status){
        timings.erase(std::remove(timings.begin(), timings.end(), ','), timings.end());
        std::bitset<24> timing(timings);
        if(status == 0){
            timing.flip();
        }
        return timing;
    }
    int hours2int(int hour){
        return 1 << (hour);
    }
    bool checkHours(uint32_t adgroup, int hour){
        return adgroup2timings[adgroup][23-hour];
    }

    //csv读取
    std::vector<uint8_t> split2int(const std::string& str, char delimiter) {
        std::vector<uint8_t> tokens;
        std::stringstream ss(str);
        std::string token;
        while (std::getline(ss, token, delimiter)) {
            tokens.push_back(std::stoi(token));
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

    void read_csv_rows(const std::string& csvFile, int startRow, int endRow ) {
        csv::CSVReader<8, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
        int rowNum = 0;
        std::cout << rowNum << " " << startRow << " " << endRow << std::endl;
        reader.set_file_line(startRow);
        int currentRow = startRow;
        uint64_t keyword,adgroup,price,campaign_id,item_id;
        uint8_t status;
        std::vector<uint8_t> timings(24, 0);
        std::pair<float, float> itemVector;
        std::string timingString, itemVectorString;

        while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
            if (keywordID.find(keyword) == keywordID.end()) {
                keywordID[keyword] = keywordID.size();
            }
            keyword = keywordID[keyword];
            if (adgroupID.find(adgroup) == adgroupID.end()) {
                adgroupID[adgroup] = adgroupID.size();
                ID2adgroup[adgroupID.size()-1] = adgroup;
            }
            adgroup = adgroupID[adgroup];
            

            if(keywordAdgroupSet.size() > keyword){
                keywordAdgroupSet[keyword].insert(adgroup);
            }
            else{
                keywordAdgroupSet.emplace_back(std::set<uint32_t>{});
                keywordAdgroupSet[keyword].insert(adgroup);
            }
            adgroup2price[adgroup] = price;

            std::bitset<24> timing = timings2bitset(timingString, status);
            adgroup2timings[adgroup] = timing;

            itemVector = split2float(itemVectorString);
            if(keywordAdgroup2vector.size() > keyword){
                keywordAdgroup2vector[keyword][adgroup] = itemVector;
            }
            else{
                keywordAdgroup2vector.emplace_back(std::map<uint32_t, std::pair<float, float> >{});
                keywordAdgroup2vector[keyword][adgroup] = itemVector;
            }
            rowNum++;
        }
    }
    // 合并两个优先队列并返回去重后的 topn 个元素的优先队列
    std::priority_queue<AdGroup> mergeAndDistinctAdGroup(Response* rp1, Response* rp2, int topn) {
        std::priority_queue<AdGroup> mergedPQ;
        std::set<AdGroup> distinctElements;
        for(int i = 0 ; i < rp1->adgroup_ids().size() ; i ++){
            AdGroup  nowAdgroup;
            nowAdgroup.adgroup_id = rp1->adgroup_ids()[i];
            nowAdgroup.score = rp1->myscores()[i];
            nowAdgroup.price = rp1->myprices()[i];
            nowAdgroup.ctr = rp1->myctrs()[i];
            auto iter = distinctElements.find(nowAdgroup);
            if(iter == distinctElements.end() || nowAdgroup < *iter){
                distinctElements.insert(nowAdgroup);
            }
            nowAdgroup.adgroup_id = rp2->adgroup_ids()[i];
            nowAdgroup.score = rp2->myscores()[i];
            nowAdgroup.price = rp2->myprices()[i];
            nowAdgroup.ctr = rp2->myctrs()[i];
            iter = distinctElements.find(nowAdgroup);
            if(iter == distinctElements.end() || nowAdgroup < *iter){
                distinctElements.insert(nowAdgroup);
            }
        }
        for(auto &it:distinctElements)
        {
            mergedPQ.push(it);
        }

        while (mergedPQ.size() > topn+1) {
            mergedPQ.pop();
        }
        return mergedPQ;
    }

    void readCsv(const std::string& path){
        int start_row = 0;  // 起始行
        int end_row = 20;  
        read_csv_rows(path, start_row, end_row);
    }
    void printPrivate(){
        std::cout << "keywordID:" << std::endl;
        for (const auto& pair : keywordID) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }

        std::cout << "adgroupID:" << std::endl;
        for (const auto& pair : adgroupID) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }

        std::cout << "keywordAdgroupSet:" << std::endl;
        for (const auto& set : keywordAdgroupSet) {
            for (const auto& value : set) {
                std::cout << value << " ";
            }
            std::cout << std::endl;
        }

        std::cout << "keywordAdgroup2vector:" << std::endl;
        for (const auto& map : keywordAdgroup2vector) {
            for (const auto& pair : map) {
                std::cout << pair.first << ": (" << ID2adgroup[pair.second.first] << ", " << pair.second.second << ") ";
            }
            std::cout << std::endl;
        }

        std::cout << "adgroup2price:" << std::endl;
        for (const auto& pair : adgroup2price) {
            std::cout << ID2adgroup[pair.first] << ": " << pair.second << std::endl;
        }

        std::cout << "adgroup2timings:" << std::endl;
        for (const auto& pair : adgroup2timings) {
            std::cout << ID2adgroup[pair.first] << ": " << pair.second << std::endl;
        }
    }
    SearchServiceImpl() {

        stub_node2 = SearchService::NewStub(grpc::CreateChannel("node-2:50051", grpc::InsecureChannelCredentials()));
        stub_node3 = SearchService::NewStub(grpc::CreateChannel("node-3:50051", grpc::InsecureChannelCredentials()));

        std::cout << "开始读取csv" << std::endl;
        readCsv("/data/raw_data.csv");
        std::cout << "读取csv成功" << std::endl;
        printPrivate();
    }

    float dot_product(const std::pair<float, float>& A, const std::pair<float, float>& B) {
        return A.first * B.first + A.second * B.second;
    }

    float magnitude(const std::pair<float, float>& v) {
        return std::sqrt(v.first * v.first + v.second * v.second);
    }

    float getCtr(const std::pair<float, float>& A, const std::pair<float, float>& B) {
        float dot = dot_product(A, B);
        float magA = magnitude(A);
        float magB = magnitude(B);

        if (magA == 0.0 || magB == 0.0) {
            // Avoid division by zero
            return 0.000001f;
        }

        return dot / (magA * magB) + 0.000001f;
    }
    
    Status Search(ServerContext* context, const Request* request, Response* response) override {

        std::cout << "正在向其他主机读取信息..."<< std::endl;

        Response* rp1;
        Response* rp2;

        Status status2 = stub_node2->Search(&context2, *request, rp1);
        Status status3 = stub_node3->Search(&context3, *request, rp2);
        // todo 错误处理，及时发现从机错误
        
        int topn = request->topn();
        
        // 合并俩response  合并两个优先队列并返回去重后的 topn 个元素的优先队列
        std::priority_queue<AdGroup> adGroupPQ = mergeAndDistinctAdGroup(rp1, rp2, topn);

        // 合并完了就是可行集合需要维护解
        uint64_t responseAdgroup[topn+1];
        uint64_t responsePrice[topn+1];
        std::cout<<"adGroupPQ.size()"<<adGroupPQ.size()<<std::endl;
        if(adGroupPQ.size() <= 1){
            return Status::OK;
        }
        AdGroup nowAdgroup = adGroupPQ.top();
        float score = nowAdgroup.score;
        if(adGroupPQ.size() >= topn+1){
            adGroupPQ.pop();
        }
        int PQsize = adGroupPQ.size();
        std::cout<<"PQsize"<<PQsize<<std::endl;
        for(int i = PQsize-1 ; i >= 0 ; i--){
            nowAdgroup = adGroupPQ.top();
            adGroupPQ.pop();
            std::cout<<"nowAdgroup.adgroup_id"<<nowAdgroup.adgroup_id<<std::endl;
            responseAdgroup[i] = nowAdgroup.adgroup_id;
            responsePrice[i] = score/nowAdgroup.ctr + 0.5;
            score = nowAdgroup.score;
        }

        for(int i = 0 ; i < PQsize ; i++){
            response->add_adgroup_ids(responseAdgroup[i]);
            response->add_prices(responsePrice[i]);
        }
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

    //node-1监听
    RunServer();
    //node-2访问
    // std::string diskCache = "node-1:50051";
    // std::unique_ptr<SearchService::Stub> stub_(SearchService::NewStub(grpc::CreateChannel(diskCache, grpc::InsecureChannelCredentials())));
    // Request request;

    // request.add_keywords(2916200016);
    // request.add_context_vector(0.351177);
    // request.add_context_vector(0.936309);
    // request.set_hour(7);
    // request.set_topn(2);

    // Response reply;
    // grpc::ClientContext context;
    // std::cout << "正在读取..."<< std::endl;
    // Status status = stub_->Search(&context, request, &reply);
    // for(const auto& adgroup_id : reply.adgroup_ids())
    // {
    //     std::cout << "adgroup_id:"<< adgroup_id << std::endl;
    // }
    // for(const auto& price : reply.prices())
    // {
    //     std::cout << "price:"<< price << std::endl;
    // }


    return 0;
}