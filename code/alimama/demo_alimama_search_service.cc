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
using grpc::ClientContext;
using grpc::ClientAsyncResponseReader;
using grpc::ServerAsyncResponseWriter;
using grpc::CompletionQueue;


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


// bool printFlag = true;
// const uint32_t keywordCount = 1e5+500;
// const uint32_t adgroupCount = 5e5+500;
// const uint32_t averageAdgroupPerKeyword = 5;
// const uint32_t averageKeywordPerAdgroup = 1;
bool printFlag = false;
const uint32_t keywordCount = 1e6;
const uint32_t adgroupCount = 5e6;
const uint32_t averageAdgroupPerKeyword = 500;
const uint32_t averageKeywordPerAdgroup = 100;

// std::unordered_map<uint64_t, uint32_t> keywordID(keywordCount);
// std::unordered_map<uint64_t, uint32_t> adgroupID(adgroupCount);
// std::vector<uint64_t> ID2adgroup(adgroupCount);
// std::vector<std::unordered_set<uint32_t> > keywordAdgroupSet(keywordCount, std::unordered_set<uint32_t>{});

// std::vector<std::string> adgroup2timingstring(adgroupCount, std::string(50, '\0'));
// std::vector<uint8_t> adgroup2status(adgroupCount);
// std::vector<std::unordered_map<uint32_t, std::string > > adgroupKeyWord2vectorstring(adgroupCount, std::unordered_map<uint32_t, std::string>{});
// std::vector<std::unordered_map<uint32_t, uint32_t> > adgroupKeyword2price(adgroupCount, std::unordered_map<uint32_t, uint32_t>{});

std::unique_ptr<std::unordered_map<uint64_t, uint32_t>> keywordIDPtr;
std::unique_ptr<std::unordered_map<uint64_t, uint32_t>> adgroupIDPtr;
std::unique_ptr<std::vector<uint64_t>> ID2adgroupPtr;
std::unique_ptr<std::vector<std::bitset<24>>> adgroup2timingPtr;
std::unique_ptr<std::vector<std::vector<uint16_t>>> adgroupKeyword2pricePtr;
std::unique_ptr<std::vector<std::vector<std::pair<float, float>>>> adgroupKeyword2vectorPtr;
std::unique_ptr<std::vector<std::vector<std::pair<uint32_t,uint16_t>>>> keywordAdgroupPtr;


// std::vector<std::vector<uint32_t>> adgroupKeyword2keywordIDPtr;

// std::unique_ptr<std::vector<std::string>> adgroup2timingstringPtr;
// std::unique_ptr<std::vector<uint8_t>> adgroup2statusPtr;
// std::unique_ptr<std::vector<std::unordered_map<uint32_t, uint32_t>>> adgroupKeyword2pricePtr;
// std::unique_ptr<std::vector<std::unordered_map<uint32_t, std::pair<float,float>>>> adgroupKeyword2vectorPtr;

std::shared_mutex keywordIDMutex;
std::shared_mutex adgroupIDMutex;
std::shared_mutex ID2adgroupMutex;
std::shared_mutex keywordAdgroupMutex;
std::shared_mutex adgroupKeywordMutex;

class ThreadPool {
public:
    ThreadPool(size_t numThreads) : stop(false) {
        for (size_t i = 0; i < numThreads; ++i)
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queueMutex);
                        condition.wait(lock, [this] { return stop || !tasks.empty(); });
                        if (stop && tasks.empty())
                            return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                    {
                        std::unique_lock<std::mutex> lock(counterMutex);
                        --taskCount;
                        if (taskCount == 0) {
                            counterCondition.notify_one();
                        }
                    }
                }
            });
    }

    template <class F, class... Args>
    void enqueue(F&& f, Args&&... args) {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            tasks.emplace([f, args...] { f(args...); });
        }
        {
            std::unique_lock<std::mutex> lock(counterMutex);
            ++taskCount;
        }
        condition.notify_one();
    }

    void wait() {
        std::unique_lock<std::mutex> lock(counterMutex);
        counterCondition.wait(lock, [this] { return taskCount == 0; });
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& worker : workers)
            worker.join();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;

    std::mutex counterMutex;
    std::condition_variable counterCondition;
    size_t taskCount = 0;
};

struct CompareAdgroupMessage {
    bool operator()(const AdgroupMessage& a, const AdgroupMessage& b) const {
        if (std::abs(a.score() - b.score()) > 1e-6) {
            return a.score() > b.score();
        }

        if (std::abs(a.price() - b.price()) > 1e-6) {
            return a.price() < b.price();
        }

        return a.adgroup_id() > b.adgroup_id();
    }
};

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
                for (int i = 0; i < 2; i++){
                std::string distNode = "node-" + std::to_string(i+2) + ":50051";
                stub_node[i] = SearchService::NewStub(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
            }
        }
        readCsv("/data/raw_data.csv");
        if(printFlag){printPrivate();}
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
                
                ClientContext tempContext;
                status = stub_node[i]->CheckAvailability(&tempContext, request[i], &response[i]);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if(printFlag){
                    std::cout << "status.ok()" << status.ok() << "response.available()" << response[i].available() << std::endl;
                }
            }
        }
    }

    static std::bitset<24> timings2bitset(std::string& timings, uint8_t status){
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
    static bool checkHours(uint32_t adgroup, int hour){
        // return (*adgroup2timingstringPtr)[adgroup][hour*2]-'0' == (*adgroup2statusPtr)[adgroup];
        return (*adgroup2timingPtr)[adgroup][23-hour];
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
    static void split2float(const std::string& str, std::pair<float, float>& result) {
        std::stringstream ss(str);
        ss >> result.first;
        ss.ignore(); // 忽略逗号或其他分隔符
        ss >> result.second;
    }

    void read_csv_map_pool(const std::string& csvFile, int startRow, int endRow , int threadNumber, int threadCount) {
        csv::CSVReader<8, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
        if(printFlag){
            std::cout  << " " << startRow << " " << endRow << std::endl;
        }
        int currentRow = 0;
        uint64_t keyword,adgroup,price,campaign_id,item_id;
        uint8_t status;
        bool newadgroup;
        std::string timingString, itemVectorString;
        std::bitset<24> timing;
        std::pair<float, float> itemVector;
        uint8_t keywordPosition = 0;
        uint32_t adgroupid,keywordid;
        while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
            
            {
                std::unique_lock<std::shared_mutex> lock(keywordIDMutex);
                if (keywordIDPtr->find(keyword) == keywordIDPtr->end()) {
                    (*keywordIDPtr)[keyword] = keywordIDPtr->size();
                }
            }
            {
                std::shared_lock<std::shared_mutex> lock(keywordIDMutex);
                keywordid = (*keywordIDPtr)[keyword];
            }

            if(adgroup%3 != hostNode-1 || (adgroup/3) % threadCount != threadNumber){
                currentRow++;
                continue;
            }

            newadgroup = false;

            
            {
                std::unique_lock<std::shared_mutex> lock(adgroupIDMutex);
                if(adgroupIDPtr->find(adgroup) == adgroupIDPtr->end()){
                    (*adgroupIDPtr)[adgroup] = (*adgroupIDPtr).size();
                    newadgroup = true;
                }
            }
            {
                std::shared_lock<std::shared_mutex> lock(adgroupIDMutex);
                adgroupid = (*adgroupIDPtr)[adgroup];
            }
            
            if(newadgroup)
            {
                std::unique_lock<std::shared_mutex> lock(ID2adgroupMutex);
                timing = timings2bitset(timingString, status);
                if(ID2adgroupPtr->size() <= adgroupid){
                    (*ID2adgroupPtr).emplace_back(adgroup);
                    (*adgroup2timingPtr).emplace_back(timing);
                }
                else{
                    (*ID2adgroupPtr)[adgroupid] = adgroup;
                    (*adgroup2timingPtr)[adgroupid] = timing;
                }
            }

            


            split2float(itemVectorString, itemVector);
            {
                std::unique_lock<std::shared_mutex> lock(adgroupKeywordMutex);
                if(adgroupKeyword2pricePtr->size() <= adgroupid){
                    (*adgroupKeyword2pricePtr).emplace_back(std::vector<uint16_t>());
                    (*adgroupKeyword2vectorPtr).emplace_back(std::vector<std::pair<float, float>>()); 
                }
            }
            {
                std::unique_lock<std::shared_mutex> lock(keywordAdgroupMutex);
                if(keywordAdgroupPtr->size() <= keywordid){
                    (*keywordAdgroupPtr).emplace_back(std::vector<std::pair<uint32_t,uint16_t>>());
                }
                (*keywordAdgroupPtr)[keywordid].emplace_back(adgroupid, (*adgroupKeyword2pricePtr)[adgroupid].size());
            }
            
            (*adgroupKeyword2pricePtr)[adgroupid].emplace_back(price);
            (*adgroupKeyword2vectorPtr)[adgroupid].emplace_back(itemVector);
            currentRow++;

        }
    }
  

    
    void readCsv(const std::string& path){
        int len = 700000000;
        int startRow = 0;  // 起始行
        int endRow = len;  
        int threadCount = 16;
        std::vector<std::thread> threads;
        for (int i = 0; i < threadCount; ++i) {
            threads.emplace_back([this, path, startRow, endRow, i, threadCount]() {
                read_csv_map_pool(path, startRow, endRow, i, threadCount);
            });
        }
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
    }

    void printPrivate(){
        //打印 keywordIDPtr
        std::cout << "keywordIDPtr:" << std::endl;
        for (const auto& item : *keywordIDPtr) {
            std::cout << item.first << " -> " << item.second << std::endl;
        }
        
        // 打印 adgroupIDPtr
        // std::cout << "adgroupIDPtr:" << std::endl;
        // for (const auto& item : *adgroupIDPtr) {
        //     std::cout << item.first << " -> " << item.second << std::endl;
        // }
        
        // // 打印 ID2adgroupPtr
        // std::cout << "ID2adgroupPtr:" << std::endl;
        // for (const auto& item : *ID2adgroupPtr) {
        //     std::cout << item << std::endl;
        // }
        
        // 打印 keywordAdgroupSetPtr
        // std::cout << "keywordAdgroupSetPtr:" << std::endl;
        // for (const auto& item : *keywordAdgroupSetPtr) {
        //     if(item.size() == 0){
        //         break;
        //     }
        //     std::cout << "{ ";
        //     for (const auto& adgroup : item) {
        //         std::cout << adgroup << " ";
        //     }
            
        //     std::cout << "}" << std::endl;
        // }
        
        // // 打印 adgroup2timingstringPtr
        // std::cout << "adgroup2timingstringPtr:" << std::endl;
        // for (const auto& item : *adgroup2timingstringPtr) {
        //     std::cout << item << std::endl;
        // }
        
        // // 打印 adgroup2statusPtr
        // std::cout << "adgroup2statusPtr:" << std::endl;
        // for (const auto& item : *adgroup2statusPtr) {
        //     std::cout << static_cast<int>(item) << std::endl;
        // }
        
        // 打印 adgroupKeyword2pricePtr
        // std::cout << "adgroupKeyword2pricePtr:" << std::endl;
        // int index = 0;
        // for (const auto& map : *adgroupKeyword2pricePtr) {
        //     if(map.size() == 0){
        //         break;
        //     }
        //     std::cout << index << std::endl;
        //     for (const auto& item : map) {
        //         std::cout << item.first << " -> " << item.second << std::endl;
        //     }
        //     index++;
        // }
        
        // // 打印 adgroupKeyword2vectorPtr
        // std::cout << "adgroupKeyword2vectorPtr:" << std::endl;
        // for (const auto& map : *adgroupKeyword2vectorPtr) {
        //     for (const auto& item : map) {
        //         std::cout << item.first << " -> (" << item.second.first << ", " << item.second.second << ")" << std::endl;
        //     }
        // }
    }

    static float dot_product(const std::pair<float, float>& A, const std::pair<float, float>& B) {
        return A.first * B.first + A.second * B.second;
    }

    static float magnitude(const std::pair<float, float>& v) {
        return std::sqrt(v.first * v.first + v.second * v.second);
    }

    static float getCtr(const std::pair<float, float>& A, const std::pair<float, float>& B) {
        float dot = dot_product(A, B);
        float magA = magnitude(A);
        float magB = magnitude(B);

        if (magA == 0.0 || magB == 0.0) {
            // Avoid division by zero
            return 0.000001f;
        }

        return dot / (magA * magB) + 0.000001f;
    }
    
    void getList(const Request* request, InnerResponse* response){
        if(printFlag){
        std::cout << "开始了一次get请求" << std::endl;}
        // google::protobuf::RepeatedField<float> context_vector = request->context_vector();
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        // 首先根据hour过滤出可行的字段列表
        //std::vector<std::unordered_set<uint32_t> > adgroupUseful(keywordLength, std::unordered_set<uint32_t>{});
        if(printFlag){
        std::cout << "首先根据hour过滤出可行的字段列表" << std::endl;}
        std::unordered_map<uint32_t, uint16_t> adgroupmap;
        std::vector<std::vector<uint16_t>> adgroupPosition;
        for(const auto & userKeyword : request->keywords()){
            // std::cout << "userKeyword" << userKeyword << std::endl;
            // std::cout << "(*keywordIDPtr)[userKeyword]" << (*keywordIDPtr)[userKeyword] << std::endl;
            for(const auto& pair : (*keywordAdgroupPtr)[(*keywordIDPtr)[userKeyword]]){
                // std::cout << "pair.first(adgroupID)" << pair.first << std::endl;
                // std::cout << "pair.second(keywordposition)" << pair.second << std::endl;
                if(checkHours(pair.first, hour)){

                    if(adgroupmap.find(pair.first) == adgroupmap.end()){
                        adgroupmap[pair.first] = adgroupmap.size();
                        adgroupPosition.emplace_back(std::vector<uint16_t>());
                    }
                    adgroupPosition[adgroupmap[pair.first]].emplace_back(pair.second);
                }
            }
        }
        if(printFlag){
        std::cout << "过滤完毕" << std::endl;}
        CompareAdgroupMessage cmp;
        std::priority_queue<AdgroupMessage, std::vector<AdgroupMessage>, CompareAdgroupMessage> adGroupPQ;

        for(const auto& pair : adgroupmap){
            //adgroupid = pair.first;
            // adgroupPosition[pair.second] keyword在不同adgroup下的下标
            if(adgroupPosition[pair.second].size() == keywordLength){
                AdgroupMessage nowAdgroup;
                nowAdgroup.set_score(std::numeric_limits<float>::lowest());
                int index = 0;
                for(const auto& pos : adgroupPosition[pair.second]){
                    // auto& userkeyword = request->keywords()[index];
                    // itemvector = (*adgroupKeyword2vectorPtr)[adgroupid][pos];
                    float ctr = getCtr(userVector, (*adgroupKeyword2vectorPtr)[pair.first][pos]);
                    uint16_t price = (*adgroupKeyword2pricePtr)[pair.first][pos];
                    float score = ctr*price;
                    if(score > nowAdgroup.score() || (score == nowAdgroup.score() && price < nowAdgroup.price() )){
                        nowAdgroup.set_adgroup_id((*ID2adgroupPtr)[pair.first]);
                        nowAdgroup.set_ctr(ctr);
                        nowAdgroup.set_price(price);
                        nowAdgroup.set_score(score);
                    }
                    index++;
                }
                if(adGroupPQ.size() < topn+1){
                    adGroupPQ.push(nowAdgroup);
                }
                else if(cmp(nowAdgroup,adGroupPQ.top())){
                    adGroupPQ.pop();
                    adGroupPQ.push(nowAdgroup);
                }

            }
        }
        while (!adGroupPQ.empty()) {
            AdgroupMessage topAdgroup = adGroupPQ.top();
            adGroupPQ.pop();
            AdgroupMessage* adgroup = response->add_adgroups();
            *adgroup = topAdgroup;
            
        }
        if(printFlag){
        std::cout << "结束了一次get请求" << std::endl;}

    }
    
    Status Get(ServerContext * context, const Request* request, InnerResponse* response) override {
        getList(request, response);
        return Status::OK;
    }

        // 合并两个优先队列并返回去重后的 topn 个元素的优先队列
    void mergeAndDistinctAdGroup(InnerResponse& rp1, InnerResponse& rp2, InnerResponse& rp3, int topn, std::vector<AdgroupMessage>& res, int & index) {
        
        AdgroupMessage nowAdgroupMessage;
        nowAdgroupMessage.set_adgroup_id(-1);
        nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
        int index1 = rp1.adgroups().size()-1, index2 = rp2.adgroups().size()-1, index3 = rp3.adgroups().size()-1;
        while((index < topn+1)&& (index1 >= 0 || index2 >= 0 || index3 >= 0)){
            if(index1 >= 0){
                nowAdgroupMessage = std::min(nowAdgroupMessage, rp1.adgroups()[index1],CompareAdgroupMessage());
            }
            if(index2 >= 0){
                nowAdgroupMessage = std::min(nowAdgroupMessage, rp2.adgroups()[index2],CompareAdgroupMessage());
            }
            if(index3 >= 0){
                nowAdgroupMessage = std::min(nowAdgroupMessage, rp3.adgroups()[index3],CompareAdgroupMessage());
            }
            res[index++] = nowAdgroupMessage;
            if (index1 >= 0 && rp1.adgroups()[index1].adgroup_id() == nowAdgroupMessage.adgroup_id()) {
                --index1;
                if(index1 >= 0)
                {
                    nowAdgroupMessage = rp1.adgroups()[index1];
                }
                else 
                {
                    nowAdgroupMessage.set_adgroup_id(-1);
                    nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
                }
            }
            if (index2 >= 0 && rp2.adgroups()[index2].adgroup_id() == nowAdgroupMessage.adgroup_id()) {
                --index2;
                if(index2 >= 0)
                {
                    nowAdgroupMessage = rp2.adgroups()[index2];
                }
                else 
                {
                    nowAdgroupMessage.set_adgroup_id(-1);
                    nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
                }
            }
            if (index3 >= 0 && rp3.adgroups()[index3].adgroup_id() == nowAdgroupMessage.adgroup_id()) {
                --index3;
                if(index3 >= 0)
                {
                    nowAdgroupMessage = rp3.adgroups()[index3];
                }
                else 
                {
                    nowAdgroupMessage.set_adgroup_id(-1);
                    nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
                }
            }

        }
        // int index1 = 0, index2 = 0, index3 = 0;
        // while((index < topn+1) &&(index1 < rp1.adgroups().size() || index2 < rp2.adgroups().size() || index3 < rp3.adgroups().size()))
        // {
        //     // 比大小
        //     if(index1 < rp1.adgroups().size())
        //     {
        //         nowAdgroupMessage = std::min(nowAdgroupMessage, rp1.adgroups()[index1],CompareAdgroupMessage());
        //     }
        //     if(index2 < rp2.adgroups().size())
        //     {
        //         nowAdgroupMessage = std::min(nowAdgroupMessage, rp2.adgroups()[index2],CompareAdgroupMessage());
        //     }
        //     if(index3 < rp3.adgroups().size())
        //     {
        //         nowAdgroupMessage = std::min(nowAdgroupMessage, rp3.adgroups()[index3],CompareAdgroupMessage());
        //     }
        //     // 进res
        //     res[index++] = nowAdgroupMessage;
        //     // res[index].set_adgroup_id = nowAdgroupMessage.adgroup_id();
        //     // res[index].set_score = nowAdgroupMessage.score();
        //     // res[index].set_price = nowAdgroupMessage.price();
        //     // res[index++].set_ctr = nowAdgroupMessage.ctr();
            
        //     // 增序
        //     if (index1 < rp1.adgroups().size() && rp1.adgroups()[index1].adgroup_id() == nowAdgroupMessage.adgroup_id()) {
        //         ++index1;
        //         if(index1 < rp1.adgroups().size())
        //         {
        //             nowAdgroupMessage = rp1.adgroups()[index1];
        //         }
        //         else 
        //         {
        //             nowAdgroupMessage.set_adgroup_id(-1);
        //             nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
        //         }
        //     }
        //     if (index2 < rp2.adgroups().size() && rp2.adgroups()[index2].adgroup_id() == nowAdgroupMessage.adgroup_id()) {
        //         ++index2;
        //         if(index2 < rp2.adgroups().size())
        //         {
        //             nowAdgroupMessage = rp2.adgroups()[index2];
        //         }
        //         else 
        //         {
        //             nowAdgroupMessage.set_adgroup_id(-1);
        //             nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
        //         }
        //     }
        //     if (index3 < rp3.adgroups().size() && rp3.adgroups()[index3].adgroup_id() == nowAdgroupMessage.adgroup_id()) {
        //         ++index3;
        //         if(index3 < rp3.adgroups().size())
        //         {
        //             nowAdgroupMessage = rp3.adgroups()[index3];
        //         }
        //         else 
        //         {
        //             nowAdgroupMessage.set_adgroup_id(-1);
        //             nowAdgroupMessage.set_score(std::numeric_limits<float>::lowest());
        //         }
        //     }
        // }
    }
    


    Status Search(ServerContext* context, const Request* request, Response* response) override {
        uint32_t topn = request->topn();
        if(printFlag){
            std::cout << "开始一次查询" << std::endl;
        }

        InnerResponse rp[3];
        getList(request, &rp[2]);
        
        Status status[2];
        grpc::ClientContext clientContext[2];
        for(int i = 0 ; i < 2 ; i++){
            status[i] = stub_node[i]->Get(&clientContext[i], *request, &rp[i]);
        }

        // for(int i = 0 ; i < 3 ; i++){
        //     std::cout << "node-" << i+1 << std::endl;
        //     for(auto& adgroup : rp[i].adgroups()){
        //         std::cout << adgroup.adgroup_id() << " " << adgroup.score() << std::endl;
        //     }
        // }
        
        if(printFlag){
            std::cout << "读取成功" << std::endl;
        }
        

        //mergeresponse(rp, response);
        std::vector<AdgroupMessage> adGroupVector(topn+1);
        int index = 0;
        mergeAndDistinctAdGroup(rp[0], rp[1], rp[2], topn, adGroupVector, index);

        // for(int i = 0 ; i < index ; i++){
        //     std::cout << "anwser:"<< std::endl;
        //     for(auto& adgroup : adGroupVector){
        //         std::cout << adgroup.adgroup_id() << " " << adgroup.score() << std::endl;
        //     }
        // }
        // std::cout << "index" << index << std:: endl;
        // std::cout << "topn" << topn << std:: endl;
        for(int i = 0 ; i < index-1 ; i++){
            response->add_adgroup_ids(adGroupVector[i].adgroup_id());
            response->add_prices(adGroupVector[i+1].score()/adGroupVector[i].ctr() + 0.5);
        }
        if(index <= topn){
            response->add_adgroup_ids(adGroupVector[index].adgroup_id());
            response->add_prices(adGroupVector[index].score()/adGroupVector[index].ctr() + 0.5);
        }

        return Status::OK;
    }
};

void RunServer() {

    // 堆区内存申请
    keywordIDPtr = std::make_unique<std::unordered_map<uint64_t, uint32_t>>(keywordCount);
    adgroupIDPtr = std::make_unique<std::unordered_map<uint64_t, uint32_t>>(adgroupCount);
    ID2adgroupPtr = std::make_unique<std::vector<uint64_t>>(adgroupCount);
    adgroup2timingPtr = std::make_unique<std::vector<std::bitset<24>>>(adgroupCount);
    keywordAdgroupPtr = std::make_unique<std::vector<std::vector<std::pair<uint32_t,uint16_t>>>> (keywordCount, std::vector<std::pair<uint32_t,uint16_t>>());
    adgroupKeyword2vectorPtr = std::make_unique<std::vector<std::vector<std::pair<float,float>>>>(adgroupCount, std::vector<std::pair<float,float>>());
    adgroupKeyword2pricePtr = std::make_unique<std::vector<std::vector<uint16_t>>>(adgroupCount, std::vector<uint16_t>());

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
