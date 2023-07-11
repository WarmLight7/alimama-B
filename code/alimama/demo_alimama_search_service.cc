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
using grpc::ClientAsyncResponseReader;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;
using alimama::proto::InnerResponse;

using alimama::proto::AdgroupMessage;
using alimama::proto::AvailabilityRequest;
using alimama::proto::AvailabilityResponse;


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
    
std::unordered_map<uint64_t, uint32_t> keywordID;
std::unordered_map<uint64_t, uint32_t> adgroupID;
std::unordered_map<uint32_t, uint64_t> ID2adgroup;
std::vector<std::unordered_set<uint32_t> > keywordAdgroupSet(1100000, std::unordered_set<uint32_t>{});
std::vector<std::unordered_map<uint32_t, std::pair<float, float> > > keywordAdgroup2vector(1100000, std::unordered_map<uint32_t, std::pair<float, float> >{}); 
std::vector<std::unordered_map<uint32_t, uint32_t> > keywordAdgroup2price(1100000, std::unordered_map<uint32_t, uint32_t>{}); 
// std::map<uint32_t, uint32_t> adgroup2price;
std::unordered_map<uint32_t, std::bitset<24> > adgroup2timings;  //使用2^24次存储 用int就够 

std::mutex keywordIDMutex;
std::mutex adgroupIDMutex;
std::mutex ID2adgroupMutex;
std::mutex keywordAdgroupSetMutex;
std::mutex keywordAdgroup2vectorMutex;
std::mutex keywordAdgroup2priceMutex;
std::mutex adgroup2timingsMutex;

bool printFlag = false;

class SearchServiceImpl final : public SearchService::Service {
private:
    bool isAvailable = false;
    std::unique_ptr<SearchService::Stub> stub_node[2];
    int hostNode;
    
public:
    SearchServiceImpl(){
        char *hostname = std::getenv("NODE_ID");
        hostNode = std::stoi(hostname);
        if(hostNode == 1){
                for (int i = 0; i <= 1; i++){
                std::string distNode = "node-" + std::to_string(i+2) + ":50051";
                stub_node[i] = SearchService::NewStub(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
            }
        }
        if(hostNode == 2 || hostNode == 3){
            readCsv("/data/raw_data.csv");
            if(printFlag){printPrivate();}
        }
        isAvailable = true;
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
            if(printFlag){
                std::cout << "status.ok()" << status.ok() << "response.available()" << response[i].available() << std::endl;
            }
            
            while(!status.ok() || !response[i].available()){
                if(printFlag){
                    std::cout << "status.ok()" << status.ok() << "response.available()" << response[i].available() << std::endl;
                }
                ClientContext tempContext;
                status = stub_node[i]->CheckAvailability(&tempContext, request[i], &response[i]);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        
        
    }
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
        if(printFlag){
            std::cout << rowNum << " " << startRow << " " << endRow << std::endl;
        }
        
        int currentRow = 0;
        uint64_t keyword,adgroup,price,campaign_id,item_id;
        uint8_t status;
        std::vector<uint8_t> timings(24, 0);
        std::pair<float, float> itemVector;
        std::string timingString, itemVectorString;
        while(currentRow < startRow){
            reader.next_line();
            currentRow++;
        }
        while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
            {
                std::lock_guard<std::mutex> keywordIDLock(keywordIDMutex);
                if (keywordID.find(keyword) == keywordID.end()) {
                    keywordID[keyword] = keywordID.size();
                }
            }
            keyword = keywordID[keyword];
            {
                std::lock_guard<std::mutex> adgroupIDLock(adgroupIDMutex);
                if (adgroupID.find(adgroup) == adgroupID.end()) {
                    adgroupID[adgroup] = adgroupID.size();
                    std::lock_guard<std::mutex> ID2adgroupLock(ID2adgroupMutex);
                    ID2adgroup[adgroupID.size() - 1] = adgroup;
                }
            }
            adgroup = adgroupID[adgroup];
            {
                std::lock_guard<std::mutex> keywordAdgroupSetLock(keywordAdgroupSetMutex);
                if (keywordAdgroupSet.size() > keyword) {
                    keywordAdgroupSet[keyword].insert(adgroup);
                }
                else {
                    keywordAdgroupSet.emplace_back(std::unordered_set<uint32_t>{});
                    keywordAdgroupSet[keyword].insert(adgroup);
                }
            }
            {
                std::lock_guard<std::mutex> keywordAdgroup2priceLock(keywordAdgroup2priceMutex);
                if (keywordAdgroup2price.size() > keyword) {
                    keywordAdgroup2price[keyword][adgroup] = price;
                }
                else {
                    keywordAdgroup2price.emplace_back(std::unordered_map<uint32_t, uint32_t>{});
                    keywordAdgroup2price[keyword][adgroup] = price;
                }
            }
            {
                std::lock_guard<std::mutex> adgroup2timingsLock(adgroup2timingsMutex);
                std::bitset<24> timing = timings2bitset(timingString, status);
                adgroup2timings[adgroup] = timing;
            }

            {
                itemVector = split2float(itemVectorString);
                std::lock_guard<std::mutex> keywordAdgroup2vectorLock(keywordAdgroup2vectorMutex);
                if (keywordAdgroup2vector.size() > keyword) {
                    keywordAdgroup2vector[keyword][adgroup] = itemVector;
                }
                else {
                    keywordAdgroup2vector.emplace_back(std::unordered_map<uint32_t, std::pair<float, float>>{});
                    keywordAdgroup2vector[keyword][adgroup] = itemVector;
                }
            }
            rowNum++;
            currentRow++;
        }
    }
   
    void readCsv(const std::string& path){
        int len = 350000000;
        int startRow = 0;  // 起始行
        int endRow = len;  
        if(hostNode == 3){
            startRow += len;
            endRow += len;
        }

        std::vector<std::thread> threads;
        int numThreads = 32; // 指定线程数

        int rowsPerThread = (endRow - startRow) / numThreads;
        int remainingRows = (endRow - startRow) % numThreads;
        int currentRow = startRow;

        for (int i = 0; i < numThreads; ++i) {
            int rowsToRead = rowsPerThread + (i < remainingRows ? 1 : 0);
            int threadEndRow = currentRow + rowsToRead;

            threads.emplace_back([this, path, currentRow, threadEndRow]() {
                read_csv_rows(path, currentRow, threadEndRow);
            });

            currentRow = threadEndRow;
        }

        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }

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
            if(set.size()){
                std::cout << std::endl;
            }
        }

        std::cout << "keywordAdgroup2vector:" << std::endl;
        for (const auto& map : keywordAdgroup2vector) {
            for (const auto& pair : map) {
                std::cout << pair.first << ": (" << ID2adgroup[pair.second.first] << ", " << pair.second.second << ") ";
            }
            if(map.size()){
                std::cout << std::endl;
            }
            
        }

        std::cout << "keywordAdgroup2price:" << std::endl;
        for (const auto& map : keywordAdgroup2price) {
            for (const auto& pair : map) {
                std::cout << pair.first << ":" << ID2adgroup[pair.second] << " ";
            }
            if(map.size()){
                std::cout << std::endl;
            }
        }

        // std::cout << "adgroup2price:" << std::endl;
        // for (const auto& pair : adgroup2price) {
        //     std::cout << ID2adgroup[pair.first] << ": " << pair.second << std::endl;
        // }

        std::cout << "adgroup2timings:" << std::endl;
        for (const auto& pair : adgroup2timings) {
            std::cout << ID2adgroup[pair.first] << ": " << pair.second << std::endl;
        }
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
    
    Status Get(ServerContext * context, const Request* request, InnerResponse* response) override {
        if(printFlag){
        std::cout << "开始了一次get请求" << std::endl;}
        google::protobuf::RepeatedField<uint64_t> userKeywords = request->keywords();
        google::protobuf::RepeatedField<float> context_vector = request->context_vector();
        std::pair<float, float> userVector = std::make_pair(context_vector[0], context_vector[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength = userKeywords.size();
        // 首先根据hour过滤出可行的字段列表
        std::vector<std::unordered_set<uint32_t> > adgroupUseful(keywordLength, std::unordered_set<uint32_t>{});
        for(int userKeywordid = 0 ; userKeywordid < keywordLength ; userKeywordid++){ // 遍历关键字
            uint64_t userKeyword = userKeywords[userKeywordid];
            for(const auto& adgroup : keywordAdgroupSet[keywordID[userKeyword]]) {  // 遍历关键字下的所有adgroupid
                if(checkHours(adgroup, hour)){
                    adgroupUseful[userKeywordid].insert(adgroup);
                }
            }
        }
        if(printFlag){
        //输出过滤后的可行字段列表
        for(int i = 0; i < adgroupUseful.size(); i++)
        {
            for(auto &j:adgroupUseful[i])
            {
                std::cout<<ID2adgroup[j]<<std::endl;
            }
        }
        }

        // 过滤完可行的字段列表之后应该要合并
        std::unordered_set<uint32_t> intersection = adgroupUseful[0];
        for (std::size_t i = 1; i < adgroupUseful.size(); ++i) {
            std::unordered_set<uint32_t> current_set = adgroupUseful[i];
            std::unordered_set<uint32_t> new_intersection;

            // 取交集nowAdgroup.price
            std::set_intersection(intersection.begin(), intersection.end(),
                                current_set.begin(), current_set.end(),
                                std::inserter(new_intersection, new_intersection.begin()));
            intersection = std::move(new_intersection);
        }

        // 合并完了就是可行集合需要维护解
        std::priority_queue<AdGroup> adGroupPQ;

        

        for (const auto& adgroup : intersection){
            AdGroup nowAdgroup;
            nowAdgroup.score = 0;
            for(const auto& userKeyword : userKeywords){
                float ctr = getCtr(userVector, keywordAdgroup2vector[keywordID[userKeyword]][adgroup]);
                float price = keywordAdgroup2price[keywordID[userKeyword]][adgroup];
                float score = ctr * price;
                if(score > nowAdgroup.score){
                    nowAdgroup.adgroup_id = ID2adgroup[adgroup];
                    nowAdgroup.ctr = ctr;
                    nowAdgroup.price = price;
                    nowAdgroup.score = score;
                }
            }
            if(adGroupPQ.size() < topn+1){
                adGroupPQ.push(nowAdgroup);
                if(printFlag){
                std::cout<<"1:"<<nowAdgroup.adgroup_id<<std::endl;
                std::cout<<"nowAdgroup.score:"<<nowAdgroup.score<<std::endl;}
            }
            else if(nowAdgroup < adGroupPQ.top()){
                adGroupPQ.pop();
                adGroupPQ.push(nowAdgroup);
                if(printFlag){
                std::cout<<"adGroupPQ.top().score:"<<adGroupPQ.top().score<<std::endl;
                std::cout<<"2:"<<nowAdgroup.adgroup_id<<std::endl;
                std::cout<<"nowAdgroup.score:"<<nowAdgroup.score<<std::endl;}
            }
        }
        int PQsize = adGroupPQ.size();
        for(int i = 0 ; i < PQsize ; i++){
            AdGroup nowAdgroup = adGroupPQ.top();
            adGroupPQ.pop();
            AdgroupMessage* adgroup = response->add_adgroups();
            adgroup->set_score(nowAdgroup.score);
            adgroup->set_price(nowAdgroup.price);
            adgroup->set_ctr(nowAdgroup.ctr);
            adgroup->set_adgroup_id(nowAdgroup.adgroup_id);
        }
        if(printFlag){
        std::cout << "结束了一次get请求" << std::endl;}
        return Status::OK;
    }
    // 合并两个优先队列并返回去重后的 topn 个元素的优先队列
    std::priority_queue<AdGroup> mergeAndDistinctAdGroup(InnerResponse& rp1, InnerResponse& rp2, int topn) {
        std::priority_queue<AdGroup> mergedPQ;
        std::set<AdGroup> distinctElements;
        AdGroup nowAdgroup;
        for(int i = 0 ; i < rp1.adgroups().size() ; i ++){
            if(printFlag){
            std::cout << rp1.adgroups()[i].adgroup_id() << " ";}
            nowAdgroup.adgroup_id = rp1.adgroups()[i].adgroup_id();
            nowAdgroup.score = rp1.adgroups()[i].score();
            nowAdgroup.price = rp1.adgroups()[i].price();
            nowAdgroup.ctr = rp1.adgroups()[i].ctr();
            auto iter = distinctElements.find(nowAdgroup);
            if(iter == distinctElements.end() || nowAdgroup < *iter){
                distinctElements.insert(nowAdgroup);
            }
        }
        if(printFlag){
        std::cout << std::endl;}
        for(int i = 0 ; i < rp2.adgroups().size() ; i ++){
            if(printFlag){
            std::cout << rp2.adgroups()[i].adgroup_id() << " ";}
            nowAdgroup.adgroup_id = rp2.adgroups()[i].adgroup_id();
            nowAdgroup.score = rp2.adgroups()[i].score();
            nowAdgroup.price = rp2.adgroups()[i].price();
            nowAdgroup.ctr = rp2.adgroups()[i].ctr();
            auto iter = distinctElements.find(nowAdgroup);
            if(iter == distinctElements.end() || nowAdgroup < *iter){
                distinctElements.insert(nowAdgroup);
            }
        }
        if(printFlag){
        std::cout << std::endl;}
        for(auto &it:distinctElements)
        {
            mergedPQ.push(it);
        }

        while (mergedPQ.size() > topn+1) {
            mergedPQ.pop();
        }
        return mergedPQ;
    }
    

    Status Search(ServerContext* context, const Request* request, Response* response) override {
        uint32_t topn = request->topn();
        if(printFlag){
            std::cout << "开始一次查询" << std::endl;
        }
        
        // std::unique_ptr<grpc::ClientAsyncResponseReader<InnerResponse>> responseReaders[2];
        // grpc::CompletionQueue cq;
        // grpc::Status status[2];
        // InnerResponse rp[2];
        // grpc::ClientContext clientContext[2];

        // for (int i = 0; i < 2; ++i) {
        //     responseReaders[i] = stub_node[i]->AsyncGet(&clientContext[i], *request, &cq);
        // }

        // bool finish = false;
        // while (!finish) {
        //     void* tag = nullptr;
        //     bool ok = false;

        //     // 等待下一个异步事件完成
        //     cq.Next(&tag, &ok);

        //     if (ok) {
        //         for (int i = 0; i < 2; ++i) {
        //             if (tag == responseReaders[i].get()) {
        //                 // 处理响应
        //                 responseReaders[i]->Finish(&rp[i], &status[i], (void*)&responseReaders[i]);
        //                 break;
        //             }
        //         }
        //     }

        //     // 判断是否所有异步请求都已完成
        //     finish = true;
        //     for (int i = 0; i < 2; ++i) {
        //         if (!status[i].ok()) {
        //             finish = false;
        //             break;
        //         }
        //     }
        // }

        


        InnerResponse rp[2];
        Status status[2];
        grpc::ClientContext clientContext[2];
        for(int i = 0 ; i < 2 ; i++){
            status[i] = stub_node[i]->Get(&clientContext[i], *request, &rp[i]);

        }

        if(printFlag){
            std::cout << "读取成功" << std::endl;
        }
        std::priority_queue<AdGroup> adGroupPQ = mergeAndDistinctAdGroup(rp[0], rp[1], topn);

        uint64_t responseAdgroup[topn+1];
        uint64_t responsePrice[topn+1];
        if(adGroupPQ.size() < 1){
            return Status::OK;
        }
        AdGroup nowAdgroup = adGroupPQ.top();
        if(printFlag){
        std::cout << "nowAdgroup.adgroup_id "<< nowAdgroup.adgroup_id <<std::endl;}
        float score = nowAdgroup.score;
        if(adGroupPQ.size() >= topn+1){
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

    char *hostname = std::getenv("NODE_ID");
    int hostNode = std::stoi(hostname);
    std::cout << "hostNode:" << hostNode << std::endl;
    std::cout << "external_address " << external_address << std::endl;
    if(hostNode == 1){
        //创建一个etcd客户端
        service.WaitService();
        etcd::Client etcd("http://etcd:2379");

        // 将服务地址注册到etcd中mashide
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
