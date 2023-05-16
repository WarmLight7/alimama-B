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

struct TestResultSummary{
  int32_t total_num;
  int32_t ad_correct_num; // 集合和顺序都正确
  int32_t ad_partial_correct_num; // 集合正确，顺序不正确
  int32_t price_correct_num; // 价格正确
};

using TestCasesGenerator = std::function<Request()>;
using SearchServiceGprcBenchmark = GrpcBenchmark<Request, Response, TestResultSummary>;

void dump_summary(const SearchServiceGprcBenchmark::SummaryType& summary, double qps) {
  BOOST_LOG_TRIVIAL(info)  << "summary completed_requests " << summary.completed_requests << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary avg_latency_ms " << summary.avg_latency_ms << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary qps " << qps << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary error_request_count " << summary.error_request_count << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary success_request_count " << summary.success_request_count << std::endl;
  BOOST_LOG_TRIVIAL(info)  << "summary timeout_request_count " << summary.timeout_request_count << std::endl;
}

bool compare_result_dummy(const Response& resp, const Response& ref, TestResultSummary& result) {
  return true;
}

bool compare_result(const Response& resp, const Response& ref, TestResultSummary& result) {
  // Check adgroup_ids
  if (ref.adgroup_ids().size() == 0 && resp.adgroup_ids().size() == 0) {
    result.ad_correct_num ++;
  } else if (ref.adgroup_ids().size() != resp.adgroup_ids().size()) {
  } else if (std::equal(resp.adgroup_ids().begin(), resp.adgroup_ids().end(), ref.adgroup_ids().begin(), ref.adgroup_ids().end())) {
    result.ad_correct_num ++;
  } else if (std::is_permutation(resp.adgroup_ids().begin(), resp.adgroup_ids().end(), ref.adgroup_ids().begin())) {
    result.ad_partial_correct_num ++;
  }

  // Check prices
  if (ref.prices().size() == 0 && resp.prices().size() == 0) {
    result.price_correct_num ++;
  } else if (ref.prices().size() != resp.prices().size()) {
  } else if (std::equal(resp.prices().begin(), resp.prices().end(), ref.prices().begin(), ref.prices().end())) {
    result.price_correct_num ++;
  }
  result.total_num ++;
  return true;
}

void TestResultScore(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader,const TestResultConfig& config,
    SearchServiceGprcBenchmark::SummaryType& summary, double& qpsBaseLine) {
  SearchServiceGprcBenchmark bench(doreqeust, compare_result, config.thread_num, config.timeout_ms, config.qps_limit);
  auto start = std::chrono::steady_clock::now();
  BOOST_LOG_TRIVIAL(trace)  << "TestResultScore request num " << config.request_times;
  for(size_t i=0; i<config.request_times; i++) {
    TestCasePair pair{};
    reader.pop(pair);
    bench.Request(i, pair.req, pair.response);
  }
  bench.WaitAll();

  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
  summary = bench.Summary();
  qpsBaseLine = summary.success_request_count / (elapsedtime_ms / 1000);
  qpsBaseLine = 1 / (summary.avg_latency_ms / 1000);
  BOOST_LOG_TRIVIAL(info)  << "qpsBaseLine " << qpsBaseLine;

  if (summary.success_request_count < config.sample_num) {
      BOOST_LOG_TRIVIAL(error)  << "user success_request_count less than the threshold " << summary.success_request_count;
    return;
  }
  dump_summary(summary, qpsBaseLine);
  return;
}

constexpr int32_t kDefaultQpsBaseline = 500;
SearchServiceGprcBenchmark::SummaryType TestMaxQps(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader, const TestMaxQpsConfig& cfg,
    double& max_qps) {
  double qps_limit = cfg.qps_baseline > 0 ? cfg.qps_baseline : kDefaultQpsBaseline;
  double last_qps = 0;
  max_qps = 0;
  int64_t request_times = qps_limit * cfg.request_duration_each_iter_sec;

  SearchServiceGprcBenchmark::SummaryType last_summary;
  for (size_t i = 0; i<cfg.max_iter_times; ++i) {
    auto start = std::chrono::steady_clock::now();
    SearchServiceGprcBenchmark bench(doreqeust, compare_result_dummy, kThreadNum, cfg.timeout_ms, int32_t(qps_limit));
    for (size_t j = 0; j < request_times; ++j) {
      TestCasePair pair{};
      if (!reader.pop(pair)) {
        BOOST_LOG_TRIVIAL(trace)  << "pop from reader failed";
      }
      bench.Request(i, pair.req, pair.response);
    }
    bench.WaitAllUntilTimeout(cfg.request_duration_each_iter_sec * 1000);
    auto end = std::chrono::steady_clock::now();
    auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
    auto summary = bench.Summary();
    double QPS = summary.success_request_count / (elapsedtime_ms / 1000);
    last_qps = QPS;
    max_qps = last_qps > max_qps ? last_qps : max_qps;
    last_summary = summary;

    if (summary.success_request_percent < cfg.success_percent_th) {
      auto update_qps = std::min(qps_limit, QPS);
      auto diff = std::abs(update_qps - qps_limit) / qps_limit;
      if (diff < cfg.qps_step_size_percent) {
        qps_limit = qps_limit - qps_limit * cfg.qps_step_size_percent;
      } else {
        qps_limit = update_qps;
      }
    } else {
      auto update_qps = std::max(qps_limit, QPS);
      auto diff = std::abs(update_qps - qps_limit) / qps_limit;
      if (diff < cfg.qps_step_size_percent) {
        qps_limit = qps_limit + qps_limit * cfg.qps_step_size_percent;
      } else {
        qps_limit = update_qps;
      }
    }
    dump_summary(last_summary, last_qps);
  }

  dump_summary(last_summary, last_qps);
  return last_summary;
}

SearchServiceGprcBenchmark::SummaryType TestResponseTime(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader, const TestResponseTimeConfig& cfg, double& qps) {
  auto qps_limit = cfg.max_qps;
  SearchServiceGprcBenchmark bench(doreqeust, compare_result_dummy, cfg.thread_num, cfg.timeout_ms, qps_limit);
  
  int32_t request_times = qps_limit * cfg.test_duration_sec;
  auto start = std::chrono::steady_clock::now();
  for(size_t i=0; i<request_times; i++) {
    TestCasePair pair{};
    reader.pop(pair);
    bench.Request(i, pair.req, pair.response);
  }
  bench.WaitAllUntilTimeout(cfg.test_duration_sec * 1000);
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

  auto summary = bench.Summary();
  qps = summary.success_request_count / (elapsedtime_ms / 1000);
  return summary;
}

SearchServiceGprcBenchmark::SummaryType TestServiceStabilityScore(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestCaseReader& reader, const TestStabilityConfig& cfg) {
  auto qps_limit = cfg.load_percent * cfg.max_qps;
  SearchServiceGprcBenchmark bench(doreqeust, compare_result_dummy, cfg.thread_num, cfg.timeout_ms, qps_limit);
  
  int32_t request_times = qps_limit * cfg.test_duration_sec;
  auto start = std::chrono::steady_clock::now();
  for(size_t i=0; i<request_times; i++) {
    TestCasePair pair{};
    reader.pop(pair);
    bench.Request(i, pair.req, pair.response);
  }
  bench.WaitAllUntilTimeout(cfg.test_duration_sec * 1000);
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

  auto summary = bench.Summary();
  return summary;
}
