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
using grpc::ResourceQuota;
using grpc::ChannelArguments;


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
#define pi 3.14159265358979323846

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


bool printFlag = false;
const uint32_t keywordCount = 1e6;
const uint32_t adgroupCount = 2e6;
const uint32_t averageAdgroupPerKeyword = 500;
const uint32_t averageKeywordPerAdgroup = 100;
float lowest = std::numeric_limits<float>::lowest();
std::atomic<uint64_t> searchid(0);
int tableSize = 6553500;
int bucketSize = 100000;
std::vector<int> hashtable(tableSize);

// std::unique_ptr<std::unordered_map<uint64_t, uint32_t>> keywordIDPtr;
// std::unique_ptr<std::unordered_map<uint64_t, uint32_t>> adgroupIDPtr;
// std::unique_ptr<std::vector<uint64_t>> ID2adgroupPtr;
// std::unique_ptr<std::vector<std::bitset<24>>> adgroup2timingPtr;
// std::unique_ptr<std::vector<std::vector<uint32_t>>> adgroupKeyword2pricePtr;
// std::unique_ptr<std::vector<std::vector<std::pair<float, float>>>> adgroupKeyword2vectorPtr;
// std::unique_ptr<std::vector<std::unordered_map<uint32_t, uint16_t>>> adgroupKeywordidPtr;
// std::unique_ptr<std::vector<std::vector<uint32_t>>> keywordAdgroupPtr;

std::unique_ptr<std::unordered_map<uint64_t, uint32_t>> keywordIDPtr;
std::unique_ptr<std::unordered_map<uint64_t, std::bitset<24>>> adgroup2timingPtr;
std::unique_ptr<std::vector<std::vector<uint32_t>>> keywordAdgroup2pricePtr;
std::unique_ptr<std::vector<std::vector<std::pair<float, float>>>> keywordAdgroup2vectorPtr;
std::unique_ptr<std::vector<std::vector<uint64_t>>> keywordAdgroupidPtr;


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

struct Adgroup {
    float score;
    uint32_t price;
    float ctr;
    uint64_t adgroup_id;

    // Overloading the less-than operator ("<")
    bool operator<(const Adgroup& other) const {
        if (std::abs(score - other.score) > 1e-6) {
            return score < other.score;
        }

        if (price != other.price) {
            return price > other.price;
        }

        return adgroup_id < other.adgroup_id;
    }

    // Overloading the assignment operator ("=")
    Adgroup& operator=(const Adgroup& other) {
        if (this == &other) {
            return *this; // Handling self-assignment
        }

        // Copying values from 'other' to the current object
        score = other.score;
        price = other.price;
        ctr = other.ctr;
        adgroup_id = other.adgroup_id;

        return *this;
    }
};


class SearchServiceImpl final : public SearchService::Service {
private:
    bool isAvailable = false;
    std::unique_ptr<SearchService::Stub> stub_node[2];
    ChannelArguments channel_args; // 连接参数
    int hostNode;
    
public:
    SearchServiceImpl(){
        char *hostname = std::getenv("NODE_ID");
        hostNode = std::stoi(hostname);
        int index = 0;

        for(int i = 1 ; i <= 3 ; i++){
            if(i ==  hostNode){
                continue;
            }
            std::string distNode = "node-" + std::to_string(i) + ":50051";
            stub_node[index++] = SearchService::NewStub(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
        }
        // if(hostNode == 1){
        //     for (int i = 0; i < 2; i++){
        //         std::string distNode = "node-" + std::to_string(i+2) + ":50051";
        //         stub_node[i] = SearchService::NewStub(grpc::CreateChannel(distNode, grpc::InsecureChannelCredentials()));
        //     }
        // }
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

    static std::bitset<24> timings2bitset(std::string& timings){
        timings.erase(std::remove(timings.begin(), timings.end(), ','), timings.end());
        std::bitset<24> timing(timings);
        return timing;
    }

    static bool checkHours(uint64_t adgroup, int hour){
        return (*adgroup2timingPtr)[adgroup][23-hour];
    }

    static void split2float(const std::string& str, std::pair<float, float>& result) {
        std::stringstream ss(str);
        ss >> result.first;
        ss.ignore(); // 忽略逗号或其他分隔符
        ss >> result.second;
    }

    void read_csv_map_time(const std::string& csvFile, int startRow, int endRow){
        csv::CSVReader<8, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
        if(printFlag){
            std::cout  << " " << startRow << " " << endRow << std::endl;
        }
        int currentRow = 0;
        uint64_t keyword,adgroup,price,campaign_id,item_id;
        uint8_t status;
        std::string timingString, itemVectorString;
        std::bitset<24> timing;
        while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
            if (adgroup2timingPtr->find(adgroup) == adgroup2timingPtr->end()) {
                (*adgroup2timingPtr)[adgroup] = timings2bitset(timingString);
            }
        }
    }


    void read_csv_map_pool(const std::string& csvFile, int startRow, int endRow , int threadNumber, int threadCount) {
        csv::CSVReader<8, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
        if(printFlag){
            std::cout  << " " << startRow << " " << endRow << std::endl;
        }
        int currentRow = 0;
        uint64_t keyword,adgroup,price,campaign_id,item_id;
        uint8_t status;
        std::string timingString, itemVectorString;
        std::pair<float, float> itemVector;
        uint32_t keywordid;
        while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
            // if(keyword%3 != hostNode-1 || (keyword/3) % threadCount != threadNumber || status == 0){
            if(keyword % threadCount != threadNumber || status == 0){
                currentRow++;
                continue;
            }
            
            {
                std::shared_lock<std::shared_mutex> readLock(keywordIDMutex);
                if (keywordIDPtr->find(keyword) == keywordIDPtr->end()) {
                    readLock.unlock(); 
                    std::unique_lock<std::shared_mutex> writeLock(keywordIDMutex);
                    (*keywordIDPtr)[keyword] = keywordIDPtr->size();
                }
            }
            {
                std::shared_lock<std::shared_mutex> readLock(keywordIDMutex);
                keywordid = (*keywordIDPtr)[keyword];
            }
            {
                std::shared_lock<std::shared_mutex> readLock(adgroupKeywordMutex);
                if (keywordAdgroup2pricePtr->size() <= keywordid) {
                    readLock.unlock(); 
                    std::unique_lock<std::shared_mutex> writeLock(adgroupKeywordMutex);
                    while(keywordAdgroup2pricePtr->size() <= keywordid){
                        (*keywordAdgroup2pricePtr).emplace_back(std::vector<uint32_t>());
                        (*keywordAdgroup2vectorPtr).emplace_back(std::vector<std::pair<float, float>>()); 
                        (*keywordAdgroupidPtr).emplace_back(std::vector<uint64_t>());
                    }
                }
            }
            split2float(itemVectorString, itemVector);
            (*keywordAdgroupidPtr)[keywordid].emplace_back(adgroup);
            (*keywordAdgroup2pricePtr)[keywordid].emplace_back(price);
            (*keywordAdgroup2vectorPtr)[keywordid].emplace_back(itemVector);
            
            
            currentRow++;

        }
    }
  
    void readCsv(const std::string& path){
        int len = 700000000;
        int startRow = 0;  // 起始行
        int endRow = len;  
        int threadCount = 11;
        std::vector<std::thread> threads;
        for (int i = 0; i < threadCount; ++i) {
            threads.emplace_back([this, path, startRow, endRow, i, threadCount]() {
                read_csv_map_pool(path, startRow, endRow, i, threadCount);
            });
        }
        threads.emplace_back([this, path, startRow, endRow]() {
            read_csv_map_time(path, startRow, endRow);
        });
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }


        double r = 65535;
        double area_1_4_circle = (pi /4) * r * r;
        for(int i = 0 ; i < tableSize ; i++){
            double x = 1.0*i/100;
            double area = 0.5*(r*r) * (asin(x/r)) + 0.5*sqrt(r*r-x*x)*x;
            int pos = area/area_1_4_circle * bucketSize;
            hashtable[i] = pos;
            //cout << area/area_1_4_circle * r << endl;
        }
    }

    void printPrivate(){
        int index;
        // 打印 keywordIDPtr
        std::cout << "keywordIDPtr:" << std::endl;
        for (const auto& item : *keywordIDPtr) {
            std::cout << item.first << " -> " << item.second << std::endl;
        }
    }

    static float getCtr(const std::pair<float, float>& A, const std::pair<float, float>& B) {
        return A.first * B.first + A.second * B.second + 0.000001f;
    }

    void getListBucketsort(const Request* request, Response* response){
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        std::vector<std::vector<Adgroup>> adGroupBucket(bucketSize);
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            const auto& keywordid = keywordIt->second;
            int index = 0;
            for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
                if(checkHours(adgroup, hour)){
                    Adgroup nowAdgroup;
                    nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
                    if(ctr < 0){
                        continue;
                    }
                    nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
                    nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
                    nowAdgroup.adgroup_id = adgroup;
                    if(nowAdgroup.score>65535){
                        continue;
                    }
                    adGroupBucket[hashtable[(int)(nowAdgroup.score*100)]].emplace_back(nowAdgroup);
                }
                index++;
            }
        }
        
        std::unordered_set<uint64_t> adgroupset;
        int index = 0;
        int i = 0;
        int j = 0;
        for(i = bucketSize - 1 ; i >= 0 && index < topn+1; i--){
            if(adGroupBucket[i].size() == 0){
                continue;
            }
            else if(adGroupBucket[i].size() > 1){
                std::sort(adGroupBucket[i].begin(), adGroupBucket[i].end());
            }
            
            for(j = 0 ; j < adGroupBucket[i].size() && index < topn+1 ; j++){
                if(adgroupset.find(adGroupBucket[i][j].adgroup_id) == adgroupset.end()){
                    if(index != topn){
                        response->add_adgroup_ids(adGroupBucket[i][j].adgroup_id);
                    }
                    if(index != 0){
                        response->add_prices(adGroupBucket[i][j].score/ctr+0.5);
                    }
                    ctr = adGroupBucket[i][j].ctr;
                    price = adGroupBucket[i][j].price;
                    adgroupset.insert(adGroupBucket[i][j].adgroup_id);
                    index++;
                }
            }
            if(index > topn){
                break;
            }
        }
        if(index < topn+1){
            response->add_prices(price);
        }
        
    }

    void getListQuicksort(const Request* request, Response* response){
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        std::vector<Adgroup> adGroupPQ;
        std::unordered_map<uint64_t,uint32_t> adgroupmap;
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            int index = 0;
            const auto& keywordid = keywordIt->second;
            for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
                if(checkHours(adgroup, hour)){
                    Adgroup nowAdgroup;
                    nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
                    nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
                    nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
                    nowAdgroup.adgroup_id = adgroup;
                    if(adgroupmap.find(adgroup) == adgroupmap.end()){
                        adgroupmap[adgroup] = adGroupPQ.size();
                        adGroupPQ.emplace_back(nowAdgroup);
                    }
                    else{
                        size_t adgroupIndex = adgroupmap[adgroup];
                        if (nowAdgroup < adGroupPQ[adgroupIndex]) {
                            adGroupPQ[adgroupIndex].score = nowAdgroup.score;
                            adGroupPQ[adgroupIndex].ctr = nowAdgroup.ctr;
                            adGroupPQ[adgroupIndex].price = nowAdgroup.price;
                        }
                    }
                }
                index++;
            }
        }
        size_t k = std::min((size_t)topn+1, (size_t)adGroupPQ.size());
        std::nth_element(adGroupPQ.begin(), adGroupPQ.begin() + k - 1, adGroupPQ.end());

        std::sort(adGroupPQ.begin(), adGroupPQ.begin() + k);

        
        int index = 0;
        int i = 0;
        for(i = 0 ; i < adGroupPQ.size() && index < topn ; i++){
            response->add_adgroup_ids(adGroupPQ[i].adgroup_id);
            if(index != 0){
                response->add_prices(adGroupPQ[i].score/ctr+0.5);
            }
            ctr = adGroupPQ[i].ctr;
            price = adGroupPQ[i].price;
            index++;
            
        }
        if(index < topn){
            response->add_prices(price);
        }
        else{
            response->add_prices(adGroupPQ[i].score/ctr+0.5);
        }


        if(printFlag){
        std::cout << "结束了一次get请求" << std::endl;}
    }

    void getListHeapsort(const Request* request, Response* response){
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        std::vector<Adgroup> adGroupPQ;
        std::unordered_map<uint64_t,uint32_t> adgroupmap(30001);
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            const auto& keywordid = keywordIt->second;
            int index = 0;
            for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
                if(checkHours(adgroup, hour)){
                    Adgroup nowAdgroup;
                    nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
                    nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
                    nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
                    nowAdgroup.adgroup_id = adgroup;
                    auto adgroupIt =  adgroupmap.find(adgroup);
                    if(adgroupIt == adgroupmap.end()){
                        adgroupmap[adgroup] = adGroupPQ.size();
                        adGroupPQ.emplace_back(nowAdgroup);
                    }
                    else{
                        const auto& adgroupIndex = adgroupIt->second;
                        if (adGroupPQ[adgroupIndex] < nowAdgroup) {
                            adGroupPQ[adgroupIndex] = nowAdgroup;
                        }
                    }
                }
                index++;
            }
        }
        

        // std::unordered_set<uint64_t> adgroupset;

        std::make_heap(adGroupPQ.begin(), adGroupPQ.end());
        int index = 0;
        while(!adGroupPQ.empty() && index < topn){
            // if(adgroupset.find(adGroupPQ[0].adgroup_id) == adgroupset.end()){
                
                response->add_adgroup_ids(adGroupPQ[0].adgroup_id);
                if(index != 0){
                    response->add_prices(adGroupPQ[0].score/ctr+0.5);
                }
                ctr = adGroupPQ[0].ctr;
                price = adGroupPQ[0].price;
                // adgroupset.insert(adGroupPQ[0].adgroup_id);
                index++;
            // }
            std::pop_heap(adGroupPQ.begin(), adGroupPQ.end());
            adGroupPQ.pop_back();
        }
        if(index < topn){
            response->add_prices(price);
        }
        else{
            response->add_prices(adGroupPQ[0].score/ctr+0.5);
        }
        

        if(printFlag){
        std::cout << "结束了一次get请求" << std::endl;}
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
};


void RunServer() {
    int adgroupSize = 101;
    
    // 堆区内存申请
    // keywordIDPtr = std::make_unique<std::unordered_map<uint64_t, uint32_t>>(keywordCount);
    // adgroupIDPtr = std::make_unique<std::unordered_map<uint64_t, uint32_t>>(adgroupCount);
    // ID2adgroupPtr = std::make_unique<std::vector<uint64_t>>(adgroupCount);
    // adgroup2timingPtr = std::make_unique<std::vector<std::bitset<24>>>(adgroupCount);
    // keywordAdgroupPtr = std::make_unique<std::vector<std::vector<uint32_t>>> (keywordCount, std::vector<uint32_t>());
    // adgroupKeyword2vectorPtr = std::make_unique<std::vector<std::vector<std::pair<float,float>>>>(adgroupCount, std::vector<std::pair<float,float>>());
    // adgroupKeyword2pricePtr = std::make_unique<std::vector<std::vector<uint32_t>>>(adgroupCount, std::vector<uint32_t>());
    // adgroupKeywordidPtr = std::make_unique<std::vector<std::unordered_map<uint32_t, uint16_t>>>(adgroupCount, std::unordered_map<uint32_t, uint16_t>());

    keywordIDPtr = std::make_unique<std::unordered_map<uint64_t, uint32_t>>(keywordCount);
    adgroup2timingPtr = std::make_unique<std::unordered_map<uint64_t, std::bitset<24>>>(adgroupCount);
    keywordAdgroup2pricePtr = std::make_unique<std::vector<std::vector<uint32_t>>>(keywordCount);
    keywordAdgroup2vectorPtr = std::make_unique<std::vector<std::vector<std::pair<float, float>>>>(keywordCount);
    keywordAdgroupidPtr = std::make_unique<std::vector<std::vector<uint64_t>>>(keywordCount);
    
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
