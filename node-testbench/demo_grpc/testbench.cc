#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <unordered_map>

#include <cmath>
#include <etcd/Client.hpp>
#include <boost/lockfree/queue.hpp>
#include <grpcpp/grpcpp.h>
#include "helloworld.grpc.pb.h"

#include "grpc_benchmark.h"
#include "test_case_reader.h"
#include "test_case_reader_preload.h"
#include "test_case_reader_async.h"
#include "config.h"

#define BOOST_LOG_DYN_LINK 1
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
namespace logging = boost::log;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;

using TestCasesGenerator = std::function<Request()>;
using SearchServiceGprcBenchmark = GrpcBenchmark<Request, Response>;

void dump_summary(const SearchServiceGprcBenchmark::SummaryType& summary, double qps) {
  BOOST_LOG_TRIVIAL(info)  << "summary completed_requests " << summary.completed_requests << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary avg_latency_ms " << summary.avg_latency_ms << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary qps " << qps << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary error_request_count " << summary.error_request_count << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary success_request_count " << summary.success_request_count << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary timeout_request_count " << summary.timeout_request_count << std::endl;
}

struct TestCaseResultItem{
  uint64_t id;
  int32_t ad_correct; // 0 错误, 1完全正确, 2 部份正确
  int32_t price_correct; // 0 错误, 1完全正确
};

TestCaseResultItem compare_result(uint64_t id, const Response& resp, const Response& ref) {
  auto item = TestCaseResultItem{
    id, 0, 0
  };

  // Check adgroup_ids
  if (ref.adgroup_ids().size() == 0 && resp.adgroup_ids().size() == 0) {
    item.ad_correct = 1;
  } else if (ref.adgroup_ids().size() != resp.adgroup_ids().size()) {
    item.ad_correct = 0;
  } else if (std::equal(resp.adgroup_ids().begin(), resp.adgroup_ids().end(), ref.adgroup_ids().begin(), ref.adgroup_ids().end())) {
    item.ad_correct = 1;
  } else if (std::is_permutation(resp.adgroup_ids().begin(), resp.adgroup_ids().end(), ref.adgroup_ids().begin())) {
    item.ad_correct = 2;
  }

  // Check prices
  if (std::equal(resp.prices().begin(), resp.prices().end(), ref.prices().begin(), ref.prices().end())) {
    item.price_correct = 1;
  }

  return item;
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
  BOOST_LOG_TRIVIAL(trace)  << "TestResultScore request num " << config.request_times;
  for(size_t i=0; i<config.request_times; i++) {
    TestCasePair pair{};
    reader.pop(pair);
    bench.Request(i, pair.req);
    resps_ref[i] = std::move(pair.response);
  }
  bench.WaitAll();

  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
  auto summary = bench.Summary();
  BOOST_LOG_TRIVIAL(trace)  << "WaitAll summary";

  qpsBaseLine = summary.success_request_count / (elapsedtime_ms / 1000);
  BOOST_LOG_TRIVIAL(info)  << "qpsBaseLine " << qpsBaseLine;

  qpsBaseLine = 1 / (summary.avg_latency_ms / 1000);
  BOOST_LOG_TRIVIAL(info)  << "qpsBaseLine2 " << qpsBaseLine;

  if (summary.success_request_count < config.sample_num) {
      BOOST_LOG_TRIVIAL(error)  << "user success_request_count less than the threshold " << summary.success_request_count;
    return;
  }
  
  for(const auto& item : summary.data) {
    if (item.id >= config.request_times) {
      BOOST_LOG_TRIVIAL(error)  << "invalid id: item.id >= config.request_times " << (item.id >= config.request_times);
      continue;
    }
    testResultScoreDetail.push_back(compare_result(item.id, item.response, resps_ref.at(item.id)));
  }
  BOOST_LOG_TRIVIAL(trace)  << "WaitAll summary done";

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
      if (!reader.pop(pair)) {
        BOOST_LOG_TRIVIAL(trace)  << "pop from reader failed";
      }
      bench.Request(i, pair.req);
      resps_ref[i] = std::move(pair.response);
    }
    bench.WaitAllUntilTimeout(cfg.request_duration_each_iter_sec * 1000);
    auto end = std::chrono::steady_clock::now();
    auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
    auto summary = bench.Summary();
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
    bench.Request(i, pair.req);
    resps_ref[i] = std::move(pair.response);
  }
  bench.WaitAllUntilTimeout(cfg.test_duration_sec * 1000);
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

  auto summary = bench.Summary();
  for(const auto& item : summary.data) {
    if (item.id >= request_times) {
      // error
      continue;
    }
    test_result_detail.push_back(compare_result(item.id, item.response, resps_ref.at(item.id)));
  }

  return summary;
}

void init_logging() {
  logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);
}

std::unique_ptr<SearchService::Stub> setupSearchService() {
  etcd::Client etcd("http://etcd:2379");
  auto response =  etcd.get("/services/searchservice").get();
  if (response.is_ok()) {
      BOOST_LOG_TRIVIAL(info) << "Service connected successful.";
  } else {
      BOOST_LOG_TRIVIAL(info) <<  "Service connected failed: " << response.error_message();
      return std::unique_ptr<SearchService::Stub>{};
  }
  std::string server_address = response.value().as_string();
  BOOST_LOG_TRIVIAL(info)  << "server_address " << server_address;

  std::unique_ptr<SearchService::Stub> stub(SearchService::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())));
  return stub;
}

int main(int argc, char** argv) {
  init_logging();
  BOOST_LOG_TRIVIAL(trace)  << "setup service" << std::endl;
  auto stub = setupSearchService();
  SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stub](ClientContext& ctx, Request& req, Response &resp) -> Status {
    return stub->Search(&ctx, req, &resp);
  };

  BOOST_LOG_TRIVIAL(trace)  << "reader start " << std::endl;
  auto reader = TestCaseReaderAsync("test_case_20000w.csv", 100);
  // auto reader = TestCaseReaderPreload("/dev/test_case.csv", 100);
  reader.start();

  TestResultConfig test_result_cfg {};
  TestMaxQpsConfig test_max_qps_cfg {};
  TestStabilityConfig test_stability_cfg {};
  BOOST_LOG_TRIVIAL(trace)  << "reading config " << std::endl;
  ReadConfigFromFile("config.json", test_result_cfg, test_max_qps_cfg, test_stability_cfg);

  TestResult test_result {};
  std::vector<TestCaseResultItem> test_result_score_detail {};
  double qps_baseline {};
  BOOST_LOG_TRIVIAL(trace)  << "TestResultScore " << std::endl;
  TestResultScore(doreqeust, reader, test_result_cfg, test_result_score_detail, qps_baseline);
  BOOST_LOG_TRIVIAL(info)  << "qps_baseline " << qps_baseline << std::endl;

  std::vector<TestCaseResultItem> test_max_qps_detail {};
  test_max_qps_cfg.qps_baseline = qps_baseline;
  BOOST_LOG_TRIVIAL(trace)  << "TestMaxQps " << std::endl;
  double max_qps = TestMaxQps(doreqeust, reader, test_max_qps_cfg, test_max_qps_detail);
  BOOST_LOG_TRIVIAL(info)  << "max_qps " << max_qps << std::endl;

  std::vector<TestCaseResultItem> test_service_stability_detail {};
  test_stability_cfg.max_qps = max_qps;
  BOOST_LOG_TRIVIAL(trace)  << "TestServiceStabilityScore " << std::endl;
  auto test_stability_result = TestServiceStabilityScore(doreqeust, reader, test_stability_cfg, test_service_stability_detail);
  BOOST_LOG_TRIVIAL(info)  << "max_qps " << max_qps << std::endl;

  return 0;
}
