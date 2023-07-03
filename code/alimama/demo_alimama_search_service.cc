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
    uint64_t adgroup_id;

    bool operator<(const AdGroup& other) const {
        if (score < other.score) {
            return true;
        } else if (score > other.score) {
            return false;
        }

        if (price > other.price) {
            return true;
        } else if (price < other.price) {
            return false;
        }

        return adgroup_id < other.adgroup_id;
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
        return adgroup2timings[adgroup][hour];
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
    // 合并两个长度为 topn 的优先队列为一个长度为 topn 的优先队列
    std::priority_queue<int> mergePriorityQueues(const std::priority_queue<int>& pq1, const std::priority_queue<int>& pq2, int topn) {
        std::priority_queue<int> mergedPQ;
        // 将 pq1 和 pq2 的元素逐个插入 mergedPQ
        while (!pq1.empty()) {
            mergedPQ.push(pq1.top());
            pq1.pop();
        }

        while (!pq2.empty()) {
            mergedPQ.push(pq2.top());
            pq2.pop();
        }

        // 如果 mergedPQ 的大小超过 topn，删除多余的元素
        while (mergedPQ.size() > topn) {
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
                std::cout << pair.first << ": (" << pair.second.first << ", " << pair.second.second << ") ";
            }
            std::cout << std::endl;
        }

        std::cout << "adgroup2price:" << std::endl;
        for (const auto& pair : adgroup2price) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }

        std::cout << "adgroup2timings:" << std::endl;
        for (const auto& pair : adgroup2timings) {
            std::bitset<24> binary(pair.second);
            std::cout << pair.first << ": " << binary << std::endl;
        }
    }
    SearchServiceImpl() {
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
    
    float getScore(float& ctr, int& prices){

    }
    Status Search(ServerContext* context, const Request* request, Response* response) override {
    // 作为示例，我们只是简单地返回一些假数据。
    // 假设我们找到了两个广告单元
    std::vector<uint64_t> userKeywords = request->(keywords);
    std::vector<float> context_vector = request->(context_vector);
    std::pair<float, float> userVector = std::make_pair(context_vector[0], context_vector[1]);
    uint32_t hour = request->(hour);
    uint32_t topn = request->(topn);
    
    int keywordLength = userKeywords.size();
    // 首先根据hour过滤出可行的字段列表
    std::vector<std::set<uint32_t> > adgroupUseful(keywordLength, {});
    for(const auto& userKeyword : userKeywords){
    for(int userKeywordid = 0 ; userKeywordid < keywordLength ; userKeywordid++){
        uint64_t userKeyword = userkeywords[userKeywordid];
        for(const auto& adgroup : keywordAdgroupSet[keywordID[userKeyword]]) {
            if(checkHour(adgroup, hour)){
                adgroupUseful[userKeywordid].insert(adgroup);
            }
        }
    }

    // 过滤完可行的字段列表之后应该要合并
    std::set<uint32_t> intersection = adgroupUseful[0];
    for (std::size_t i = 1; i < adgroupUseful.size(); ++i) {
        std::set<uint32_t> current_set = adgroupUseful[i];
        std::set<uint32_t> new_intersection;
        // 取交集
        std::set_intersection(intersection.begin(), intersection.end(),
                              current_set.begin(), current_set.end(),
                              std::inserter(new_intersection, new_intersection.begin()));
        intersection = std::move(new_intersection);
    }

    // 合并完了就是可行集合需要维护解
    std::priority_queue<AdGroup> adGroupPQ;

    AdGroup nowAdgroup;

    for (const auto& adgroup : intersection){
        nowAdgroup.ctr = 0;
        for(const auto& userKeyword : userKeywords){
            float ctr = getCtr(userVector, keywordAdgroup2vector[keywordID[userKeyword]][adgroup]);
            nowAdgroup.ctr = max(ctr, nowAdgroup.ctr);
        }
        nowAdgroup.price = adgroup2price[adgroup];
        nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
        nowAdgroup.adgroup_id = ID2adgroup[adgroup];
        if(adGroupPQ.size() < topn+1){
            adGroupPQ.push(nowAdgroup);
        }
        else if(adGroupPQ.top() < nowAdgroup){
            adGroupPQ.pop();
            dGroupPQ.push(nowAdgroup);
        }
    }
    
    uint64_t responseAdgroup[topn];
    uint64_t responsePrice[topn];
    
    if(adGroupPQ.size() <= 1){
        return Status::OK;
    }
    nowAdgroup = adGroupPQ.top();
    float score = nowAdgroup.score;
    if(adGroupPQ.size() >= topn){
        adGroupPQ.pop();
    }
    int PQsize = adGroupPQ.size();
    for(int i = PQsize-1 ; i >= 0 ; i--){
        nowAdgroup = adGroupPQ.top();
        adGroupPQ.pop();
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
  RunServer();

  return 0;
}
