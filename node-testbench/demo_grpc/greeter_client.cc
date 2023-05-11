#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cmath>

#include <boost/lockfree/queue.hpp>
#include <grpcpp/grpcpp.h>
#include "helloworld.grpc.pb.h"
#include <etcd/Client.hpp>
#include "grpc_benchmark.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

Request genreq() {
  std::vector<uint64_t> keywords = {1, 2, 3};
  std::vector<float> context_vector = {0.1f, 0.2f};
  uint64_t hour = 12;
  uint64_t topn = 10;

  Request request;
  for (auto keyword : keywords) {
    request.add_keywords(keyword);
  }

  for (auto value : context_vector) {
    request.add_context_vector(value);
  }

  request.set_hour(hour);
  request.set_topn(topn);
  return request;
}

using SearchServiceGprcBenchmark = GrpcBenchmark<Request, Response>;

double CalcResultScore(std::unique_ptr<SearchService::Stub>& stub) {
  SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stub](ClientContext& ctx, Request& req, Response &resp) -> Status {
    return stub->Search(&ctx, req, &resp);
  };
  SearchServiceGprcBenchmark bench(doreqeust, 20, 500, 500);
  auto start = std::chrono::steady_clock::now();
  for(size_t i=0; i<500; i++) {
    bench.request(i, request);
  }
  bench.wait_all();
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
  auto summary = bench1.summary();
  std::cout << "summary elapsedtime_ms " << elapsedtime_ms << std::endl;
  std::cout << "summary completed_requests " << summary.completed_requests << std::endl;
  std::cout << "summary QPS " << summary.success_request_count / (elapsedtime_ms / 1000) << std::endl;
  std::cout << "summary avg_latency_ms " << summary.avg_latency_ms << std::endl;
  std::cout << "summary error_request_count " << summary.error_request_count << std::endl;
  std::cout << "summary success_request_count " << summary.success_request_count << std::endl;
  std::cout << "summary timeout_request_count " << summary.timeout_request_count << std::endl;
}

constexpr int32_t kMaxTryTimes = 10;
constexpr double kQpsRollingStep = 0.1;
double TestMaxQps(std::unique_ptr<SearchService::Stub>& stub, int64_t timeout_ms, double sucess_percent, std::function<Request()> genrequest) {
  SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stub](ClientContext& ctx, Request& req, Response &resp) -> Status {
    return stub->Search(&ctx, req, &resp);
  };

  double max_qps = 500;
  double last_qps = 0;
  int64_t max_times = 5;
  SearchServiceGprcBenchmark::SummaryType last_summary;
  for (size_t i = 0; i<kMaxTryTimes; ++i) {
    auto start = std::chrono::steady_clock::now();
    SearchServiceGprcBenchmark bench(doreqeust, 20, timeout_ms, int32_t(max_qps));
    for (size_t j = 0; j < max_qps*max_times; ++j) {
      bench.request(j, genrequest());
    }
    bench.wait_all_until_timeout(max_times * 1000);
    auto end = std::chrono::steady_clock::now();
    auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
    auto summary = bench.summary();
    double QPS = summary.success_request_count / (elapsedtime_ms / 1000);
    last_qps = QPS;
    last_summary = summary;

    if (summary.success_request_percent < sucess_percent) {
      auto update_qps = std::min(max_qps, QPS);
      auto diff = std::abs(update_qps - max_qps) / max_qps;
      if (diff < kQpsRollingStep) {
        max_qps = max_qps - max_qps * kQpsRollingStep;
      } else {
        max_qps = update_qps;
      }
    } else {
      auto update_qps = std::max(max_qps, QPS);
      auto diff = std::abs(update_qps - max_qps) / max_qps;
      if (diff < kQpsRollingStep) {
        max_qps = max_qps + max_qps * kQpsRollingStep;
      } else {
        max_qps = update_qps;
      }
    }
    std::cout << "summary QPS " << last_qps << std::endl;
    std::cout << "summary completed_requests " << last_summary.completed_requests << std::endl;
    std::cout << "summary avg_latency_ms " << last_summary.avg_latency_ms << std::endl;
    std::cout << "summary error_request_count " << last_summary.error_request_count << std::endl;
    std::cout << "summary success_request_count " << last_summary.success_request_count << std::endl;
    std::cout << "summary timeout_request_count " << last_summary.timeout_request_count << std::endl;
  }

  std::cout << "summary QPS " << last_qps << std::endl;
  std::cout << "summary completed_requests " << last_summary.completed_requests << std::endl;
  std::cout << "summary avg_latency_ms " << last_summary.avg_latency_ms << std::endl;
  std::cout << "summary error_request_count " << last_summary.error_request_count << std::endl;
  std::cout << "summary success_request_count " << last_summary.success_request_count << std::endl;
  std::cout << "summary timeout_request_count " << last_summary.timeout_request_count << std::endl;

  return last_qps;
}

double CalcCapacityScore(std::unique_ptr<SearchService::Stub>& stub) {
    SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stub](ClientContext& ctx, Request& req, Response &resp) -> Status {
      return stub->Search(&ctx, req, &resp);
    };
    SearchServiceGprcBenchmark bench1(doreqeust, 1000, 500, 500);
    auto start = std::chrono::steady_clock::now();
    for(size_t i=0; i<500; i++) {
      bench1.request(i, genrequest());
    }
    bench1.wait_all();
    auto end = std::chrono::steady_clock::now();
    auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
    auto summary = bench1.summary();
    double QPS = summary.success_request_count / (elapsedtime_ms / 1000);
    auto p99 = summary.p99_latency_ms;
}

// double calcResponseTimeScore(std::unique_ptr<SearchService::Stub>& stub) {
// }

// double calcServiceScore(std::unique_ptr<SearchService::Stub>& stub) {
// }

int main(int argc, char** argv) {
    // 创建一个etcd客户端
  etcd::Client etcd("http://etcd:2379");
  // 从etcd中获取服务地址
  auto response =  etcd.get("/services/searchservice").get();
  if (response.is_ok()) {
      std::cout << "Service connected successful.\n";
  } else {
      std::cerr << "Service connected failed: " << response.error_message() << "\n";
      return -1;
  }
  std::string server_address = response.value().as_string();
  std::cout << "server_address " << server_address << std::endl;

  std::unique_ptr<SearchService::Stub> stub(SearchService::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())));
  SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stub](ClientContext& ctx, Request& req, Response &resp) -> Status {
    return stub->Search(&ctx, req, &resp);
  };

  findMaxQps(stub, 100, 0.99, genreq);

  SearchServiceGprcBenchmark bench(doreqeust, 10, 100, 50000);
  auto request = genreq();


  // double ResultScore = calcResultScore(stub);
  // double CapacityScore = calcCapacityScore(stub);
  // double ResponseTimeScore = calcResponseTimeScore(stub);
  // double ServiceScore = calcServiceScore(stub);

  return 0;
}
