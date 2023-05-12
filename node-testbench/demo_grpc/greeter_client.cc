#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <unordered_map>

#include <cmath>

#include <boost/lockfree/queue.hpp>
#include <grpcpp/grpcpp.h>
#include "helloworld.grpc.pb.h"
#include <etcd/Client.hpp>
#include "grpc_benchmark.h"
#include "test_case_reader,h"
#include "config.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

using TestCasesGenerator = std::function<Request()>;

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

void dump_summary(const SearchServiceGprcBenchmark::SummaryType& summary, double qps) {
  std::cout << "summary completed_requests " << summary.completed_requests << std::endl;
  std::cout << "summary avg_latency_ms " << summary.avg_latency_ms << std::endl;
  std::cout << "summary qps " << qps << std::endl;
  std::cout << "summary error_request_count " << summary.error_request_count << std::endl;
  std::cout << "summary success_request_count " << summary.success_request_count << std::endl;
  std::cout << "summary timeout_request_count " << summary.timeout_request_count << std::endl;
}


struct TestCaseResultItem{
  uint64_t id;
  int32_t ad_correct; // 0 错误, 1完全正确, 2 部份正确
  int32_t price_correct; // 0 错误, 1完全正确
};

TestCaseResultItem compare_result(uint64_t id, const Response& resp, const Response& ref) {
  return TestCaseResultItem{
    id, 0, 0
  };
}

struct TestResult{
  std::vector<TestCaseResultItem> testResultScoreDetail;
  double qpsBaseLine;
  SearchServiceGprcBenchmark::SummaryType testResult;
};

void TestResultScore(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader,const TestResultConfig& config,
    std::vector<TestCaseResultItem>& testResultScoreDetail, double& qpsBaseLine) {
  SearchServiceGprcBenchmark bench(doreqeust, config.thread_num, config.timeout_ms, config.qps_limit);
  auto start = std::chrono::steady_clock::now();
  std::vector<Response> resps_ref(config.request_times);
  for(size_t i=0; i<config.request_times; i++) {
    TestCasePair pair{};
    reader.pop(pair);
    bench.request(i, pair.req);
    resps_ref[i] = std::move(pair.response);
  }
  bench.wait_all();
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
  auto summary = bench.summary();

  qpsBaseLine = summary.success_request_count / (elapsedtime_ms / 1000);
  if (summary.success_request_count < config.sample_num) {
    // error
    return;
  }
  
  for(const auto& item : summary.data) {
    if (item.id >= config.request_times) {
      // error
      continue;
    }
    testResultScoreDetail.push_back(compare_result(item.id, item.response, resps_ref.at(item.id)));
  }

  dump_summary(summary, qpsBaseLine);
}

constexpr int32_t kDefaultQpsBaseline = 500;
double TestMaxQps(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader, const TestMaxQpsConfig& cfg,
    std::vector<TestCaseResultItem>& testResultScoreDetail) {
  double max_qps = cfg.qps_baseline > 0 ? cfg.qps_baseline : kDefaultQpsBaseline;
  double last_qps = 0;
  int64_t request_times = max_qps * cfg.request_duration_each_iter_sec;

  SearchServiceGprcBenchmark::SummaryType last_summary;
  std::vector<Response> resps_ref(request_times);
  for (size_t i = 0; i<cfg.max_iter_times; ++i) {
    auto start = std::chrono::steady_clock::now();
    SearchServiceGprcBenchmark bench(doreqeust, kThreadNum, cfg.timeout_ms, int32_t(max_qps));
    for (size_t j = 0; j < request_times; ++j) {
      TestCasePair pair{};
      bench.request(i, pair.req);
      resps_ref[i] = std::move(pair.response);
    }
    bench.wait_all_until_timeout(cfg.request_duration_each_iter_sec * 1000);
    auto end = std::chrono::steady_clock::now();
    auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
    auto summary = bench.summary();
    double QPS = summary.success_request_count / (elapsedtime_ms / 1000);
    last_qps = QPS;
    last_summary = summary;

    if (summary.success_request_percent < cfg.success_percent_th) {
      auto update_qps = std::min(max_qps, QPS);
      auto diff = std::abs(update_qps - max_qps) / max_qps;
      if (diff < cfg.qps_step_size_percent) {
        max_qps = max_qps - max_qps * cfg.qps_step_size_percent;
      } else {
        max_qps = update_qps;
      }
    } else {
      auto update_qps = std::max(max_qps, QPS);
      auto diff = std::abs(update_qps - max_qps) / max_qps;
      if (diff < cfg.qps_step_size_percent) {
        max_qps = max_qps + max_qps * cfg.qps_step_size_percent;
      } else {
        max_qps = update_qps;
      }
    }
    dump_summary(last_summary, last_qps);
  }

  for(const auto& item : last_summary.data) {
      if (item.id >= request_times) {
        // error
        continue;
      }
      testResultScoreDetail.push_back(compare_result(item.id, item.response, resps_ref.at(item.id)));
    }
  dump_summary(last_summary, last_qps);
  return last_qps;
}

SearchServiceGprcBenchmark::SummaryType TestServiceStabilityScore(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader, const TestStabilityConfig& cfg,
    std::vector<TestCaseResultItem>& test_result_detail) {
  auto qps_limit = cfg.load_percent * cfg.max_qps;
  SearchServiceGprcBenchmark bench(doreqeust, cfg.thread_num, cfg.timeout_ms, qps_limit);
  
  int32_t request_times = qps_limit * cfg.test_duration_sec;
  auto start = std::chrono::steady_clock::now();
  std::vector<Response> resps_ref(request_times);
  for(size_t i=0; i<request_times; i++) {
    TestCasePair pair{};
    reader.pop(pair);
    bench.request(i, pair.req);
    resps_ref[i] = std::move(pair.response);
  }
  bench.wait_all_until_timeout(cfg.test_duration_sec * 1000);
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

  auto summary = bench.summary();
  for(const auto& item : summary.data) {
    if (item.id >= request_times) {
      // error
      continue;
    }
    test_result_detail.push_back(compare_result(item.id, item.response, resps_ref.at(item.id)));
  }

  return summary;
}

std::unique_ptr<SearchService::Stub> setupSearchService() {
  etcd::Client etcd("http://etcd:2379");
  auto response =  etcd.get("/services/searchservice").get();
  if (response.is_ok()) {
      std::cout << "Service connected successful.\n";
  } else {
      std::cerr << "Service connected failed: " << response.error_message() << "\n";
      return std::unique_ptr<SearchService::Stub>{};
  }
  std::string server_address = response.value().as_string();
  std::cout << "server_address " << server_address << std::endl;

  std::unique_ptr<SearchService::Stub> stub(SearchService::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())));
  return stub;
}

int main(int argc, char** argv) {
  auto stub = setupSearchService();
  SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stub](ClientContext& ctx, Request& req, Response &resp) -> Status {
    return stub->Search(&ctx, req, &resp);
  };

  auto reader = TestCaseReader("test_case.json", 100);
  reader.start();

  TestResultConfig test_result_cfg {};
  TestMaxQpsConfig test_max_qps_cfg {};
  TestStabilityConfig test_stability_cfg {};
  read_config_from_file("config.json", test_result_cfg, test_max_qps_cfg, test_stability_cfg);


  TestResult test_result {};
  std::vector<TestCaseResultItem> test_result_score_detail {};
  double qps_baseline {};
  TestResultScore(doreqeust, reader, test_result_cfg, test_result_score_detail, qps_baseline);
  std::cout << "qps_baseline " << qps_baseline << std::endl;

  std::vector<TestCaseResultItem> test_max_qps_detail {};
  test_max_qps_cfg.qps_baseline = qps_baseline;
  double max_qps = TestMaxQps(doreqeust, reader, test_max_qps_cfg, test_max_qps_detail);
  std::cout << "max_qps " << max_qps << std::endl;

  std::vector<TestCaseResultItem> test_service_stability_detail {};
  test_stability_cfg.max_qps = max_qps;
  auto test_stability_result = TestServiceStabilityScore(doreqeust, reader, test_stability_cfg, test_service_stability_detail);
  std::cout << "max_qps " << max_qps << std::endl;

  return 0;
}
