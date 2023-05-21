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

#include "load_config.h"

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

float cosineDistance(const std::vector<float>& vec1, const std::vector<float>& vec2) {
    if (vec1.size() != vec2.size()) {
        throw std::invalid_argument("Vector sizes do not match");
    }

    float dotProduct = 0.0;
    float normVec1 = 0.0;
    float normVec2 = 0.0;

    for (std::size_t i = 0; i < vec1.size(); ++i) {
        dotProduct += vec1[i] * vec2[i];
        normVec1 += vec1[i] * vec1[i];
        normVec2 += vec2[i] * vec2[i];
    }

    normVec1 = std::sqrt(normVec1);
    normVec2 = std::sqrt(normVec2);

    // 避免除以零的情况
    if (normVec1 == 0.0 || normVec2 == 0.0) {
      return 0.0;
    }

    float cosineSim = dotProduct / (normVec1 * normVec2);
    float cosineDist = 1.0 - cosineSim;

    return cosineDist;
}

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

PromotionalPlansMap promotionalPlans{};
PromotionalUnitsMap promotionalUnits{};
ProductVectorsMap productVectors{};

struct Result {
  uint64_t group_id;
  uint64_t prices;
  uint64_t item_id;
  uint64_t keyword;
  double hit_percent;
  double score;
};

bool compareResults(const Result& r1, const Result& r2) {
    return r1.score > r2.score;
}

std::vector<Result> getTopK(const std::vector<Result>& results, int k) {
    std::vector<Result> topK;
    std::vector<Result> tempResults = results;
    std::sort(tempResults.begin(), tempResults.end(), compareResults);
    topK.assign(tempResults.begin(), tempResults.begin() + k);
    return topK;
}

class SearchServiceImpl final : public SearchService::Service {
  Status Search(ServerContext* context, const Request* request, Response* response) override {
    // 假设我们找到了两个广告单元
    response->add_adgroup_ids(12345);
    response->add_adgroup_ids(67890);

    // 对应的价格
    response->add_prices(100);
    response->add_prices(200);
    return Status::OK;

    // 在这里实现您的广告匹配逻辑。
    // 作为示例，我们只是简单地返回一些假数据。
    std::unordered_map<uint64_t, bool> keywords{};
    for(size_t i=0; i<request->keywords_size(); i++) {
      keywords.insert({request->keywords(i), true});
    }
    std::vector<float> context_vector{};
    for(size_t i=0; i<request->context_vector_size(); i++) {
      context_vector.push_back(request->context_vector(i));
    }
    uint64_t hour = request->hour();
    uint64_t topn = request->topn();

    std::unordered_map<uint64_t, bool> matched_campaign_id{};
    for(const auto& plans : promotionalPlans) {
      if (plans.second.status == 0) {
        continue;
      }
      for (const auto& time : plans.second.timings) {
        if (time == hour) {
          matched_campaign_id.insert({plans.first, true});
        }
      }
    }
    std::vector<Result> matched_adgroups{};
    for(const auto& unit :  promotionalUnits) {
      auto it = matched_campaign_id.find(unit.first);
      if (it == matched_campaign_id.end()) {
        std::cout << "error in find unit.first " << unit.first << std::endl;
        continue;
      }
      for(const auto& kw : unit.second.keyword_prices) {
        if (keywords.find(kw.keyword) != keywords.end()) {
          matched_adgroups.push_back({
            unit.second.adgroup_id,
            kw.price,
            unit.second.item_id,
            kw.keyword
          });
        }
      }
    }

    for(auto& mt : matched_adgroups) {
      auto it = productVectors.find(mt.item_id);
      if (it == productVectors.end()) {
        std::cout << "error in find product " << std::endl;
        continue;
      }

      auto kw_id = it->second.keywords.find(mt.keyword);
      if (kw_id == it->second.keywords.end()) {
        std::cout << "error in find kw " << mt.keyword << std::endl;
        continue;
      }
      
      mt.hit_percent = cosineDistance(context_vector, kw_id->second.vector);
      mt.score = mt.hit_percent * mt.prices;
    }
    std::vector<Result> topKResults = getTopK(matched_adgroups, topn);

    for (const auto& result : topKResults) {
        response->add_adgroup_ids(result.group_id);
        response->add_prices(result.prices);
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
    std::string key = std::string("/services/searchservice/") +  external_address;

    SearchServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    // 创建一个etcd客户端
    etcd::Client etcd("http://etcd:2379");

    // 将服务地址注册到etcd中
    auto response = etcd.set(key, "").get();
    if (response.is_ok()) {
        std::cout << "Service registration successful.\n";
    } else {
        std::cerr << "Service registration failed: " << response.error_message() << "\n";
    }

    std::cout << "Server listening on " << server_address  << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
  loadconfig(promotionalPlans, promotionalUnits, productVectors);
  RunServer();

  return 0;
}
