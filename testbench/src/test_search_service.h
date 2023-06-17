#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <unordered_map>
#include <random>

#include <cmath>
#include <etcd/Client.hpp>
#include <boost/lockfree/queue.hpp>
#include <grpcpp/grpcpp.h>
#include "alimama.grpc.pb.h"

#include "grpc_benchmark.h"
#include "test_case_reader.h"
#include "test_case_reader_preload.h"
#include "test_case_reader_async.h"
#include "config.h"
#include "concurent_queue.h"

#include "search_service_async_client.h"
#include "search_service_sync_client.h"
#include "search_service_comparator.h"

#include "defines.h"

void dump_summary(const SearchServiceGprcBenchmark::SummaryData& summary, double qps) {
  BOOST_LOG_TRIVIAL(info)  << "summary completed_requests " << summary.completed_requests;
  BOOST_LOG_TRIVIAL(info)  << "summary avg_latency_ms " << summary.avg_latency_ms;
  BOOST_LOG_TRIVIAL(info)  << "summary p99_latency_ms " << summary.p99_latency_ms;
  BOOST_LOG_TRIVIAL(info)  << "summary interupted " << summary.interupted;
  BOOST_LOG_TRIVIAL(info)  << "summary qps " << qps;
  BOOST_LOG_TRIVIAL(info)  << "summary error_request_count " << summary.error_request_count;
  BOOST_LOG_TRIVIAL(info)  << "summary success_request_count " << summary.success_request_count;
  BOOST_LOG_TRIVIAL(info)  << "summary success_request_percent " << summary.success_request_percent;
  BOOST_LOG_TRIVIAL(info)  << "summary timeout_request_count " << summary.timeout_request_count;
}

void TestResultScore(std::vector<std::string> services, TestCaseReader& reader,
    const TestResultConfig& cfg, SearchServiceGprcBenchmark::SummaryData& summary) {
  auto clis = SearchServiceGprcSyncClient::CreateClientsByNum(services, cfg.thread_num);
  if (clis.size() == 0) return;
  auto comparator = std::make_shared<SearchServiceComparatorAll>();
  SearchServiceGprcBenchmark bench(clis, comparator, cfg.timeout_ms, cfg.qps_limit);
  auto start = std::chrono::steady_clock::now();
  BOOST_LOG_TRIVIAL(trace)  << "TestResultScore request sample " << cfg.sample_num;
  for(size_t i=0; i<cfg.sample_num; i++) {
    TestCasePair pair{};
    if (!reader.pop(pair)) {
      BOOST_LOG_TRIVIAL(error)  << "pop data failed";
      break;
    }
    if (!bench.Request(i, pair.req, pair.response)) {
      BOOST_LOG_TRIVIAL(error)  << "request data failed";
      break;
    }
  }
  bench.WaitAll();

  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
  summary = bench.Summary();
  auto qpsBaseLine = summary.success_request_count / (elapsedtime_ms / 1000);
  BOOST_LOG_TRIVIAL(info)  << "qpsBaseLine " << qpsBaseLine << " summary.success_request_count " << summary.success_request_count
    << " elapsedtime_ms " << elapsedtime_ms;

  if (summary.success_request_count < cfg.sample_num) {
      BOOST_LOG_TRIVIAL(error)  << "user success_request_count less than the threshold " << summary.success_request_count;
    return;
  }
  dump_summary(summary, qpsBaseLine);
  return;
}

constexpr int32_t kDefaultQpsBaseline = 500;
SearchServiceGprcBenchmark::SummaryData TestMaxQps(std::vector<std::string> services, TestCaseReader& reader, const TestMaxQpsConfig& cfg,
    double& max_qps) {
  double qps_limit = cfg.qps_baseline > 0 ? cfg.qps_baseline : kDefaultQpsBaseline;
  double last_qps = 0;
  max_qps = 0;

  SearchServiceGprcBenchmark::SummaryData last_summary;
  for (size_t i = 0; i<cfg.max_iter_times; ++i) {
    auto clis = SearchServiceGprcClient::CreateClients(services, int32_t(qps_limit));
    if (clis.size() == 0) return last_summary;
    auto comparator = std::make_shared<SearchServiceComparatorDummy>();
    SearchServiceGprcBenchmark bench(clis, comparator, cfg.timeout_ms, int32_t(qps_limit));
    int64_t request_times = qps_limit * cfg.request_duration_each_iter_sec;
    auto start = std::chrono::steady_clock::now();
    double elapsedtime_popdata_ms_all = 0;
    for (size_t j = 0; j < request_times; ++j) {
      auto popdata = std::chrono::steady_clock::now();
      TestCasePair pair{};
      if (!reader.pop(pair)) {
        BOOST_LOG_TRIVIAL(error)  << "pop data failed";
        break;
      }
      auto popdataend = std::chrono::steady_clock::now();
      elapsedtime_popdata_ms_all += std::chrono::duration<double, std::milli>(popdataend - popdata).count();
      if (!bench.Request(i, pair.req, pair.response)) {
        BOOST_LOG_TRIVIAL(error)  << "request data failed";
        break;
      }
    }
    auto not_timeout = bench.WaitAllUntilTimeout(cfg.request_duration_each_iter_sec * 1000);
    auto end = std::chrono::steady_clock::now();
    auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

    auto summary = bench.Summary();
    double QPS = summary.success_request_count / (elapsedtime_ms / 1000);
    BOOST_LOG_TRIVIAL(info)  << "timeout: " << !not_timeout << " elapsedtime_ms: " << elapsedtime_ms << " elapsedtime_popdata_ms " << elapsedtime_popdata_ms_all << " QPS " << QPS ;

    last_qps = QPS;
    max_qps = last_qps > max_qps ? last_qps : max_qps;
    last_summary = summary;

    if (summary.success_request_percent < cfg.success_percent_th) {
      qps_limit = qps_limit - qps_limit * cfg.qps_step_size_percent;
    } else {
      qps_limit = qps_limit + qps_limit * cfg.qps_step_size_percent;
    }
    dump_summary(last_summary, last_qps);
  }

  dump_summary(last_summary, last_qps);
  return last_summary;
}


SearchServiceGprcBenchmark::SummaryData TestCapacityScore(std::vector<std::string> services, TestCaseReader& reader, const TestCapacityConfig& cfg) {
  auto clis = SearchServiceGprcSyncClient::CreateClientsByNum(services, cfg.thread_num);
  if (clis.size() == 0) return {};
  // auto clis = SearchServiceGprcClient::CreateClients(services, 20000);
  // if (clis.size() == 0) return {};
  auto comparator = std::make_shared<SearchServiceComparatorSample>(cfg.sample_percent_th, cfg.sample_score_th);
  SearchServiceGprcBenchmark bench(clis, comparator, cfg.timeout_ms, 0);
  
  auto start = std::chrono::steady_clock::now();
  for(size_t i=0 ; i<cfg.sample_num; i++) {
    TestCasePair pair{};
    if (!reader.pop(pair)) {
      BOOST_LOG_TRIVIAL(error)  << "pop data failed";
      break;
    }
    if (!bench.Request(i, pair.req, pair.response)) {
      BOOST_LOG_TRIVIAL(error)  << "request data failed";
      break;
    }
  }
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();
  BOOST_LOG_TRIVIAL(info)  << "request num " << cfg.sample_num << " use " << elapsedtime_ms << " ms";
  bench.WaitAllUntilTimeout(cfg.test_duration_sec * 1000 - int32_t(elapsedtime_ms));

  auto end2 = std::chrono::steady_clock::now();
  auto elapsedtime_ms2 = std::chrono::duration<double, std::milli>(end2 - start).count();

  auto summary = bench.Summary();
  auto qps = summary.success_request_count / (elapsedtime_ms2 / 1000);
  dump_summary(summary, qps);
  summary.custom_summary.qps = qps;
  return summary;
}

SearchServiceGprcBenchmark::SummaryData TestResponseTime(std::vector<std::string> services, TestCaseReader& reader, const TestResponseTimeConfig& cfg, double& qps) {
  auto qps_limit = cfg.max_qps;
  auto clis = SearchServiceGprcClient::CreateClients(services, qps_limit);
  if (clis.size() == 0) return {};
  auto comparator = std::make_shared<SearchServiceComparatorDummy>();
  SearchServiceGprcBenchmark bench(clis, comparator, cfg.timeout_ms, qps_limit);
  
  int32_t request_times = qps_limit * cfg.test_duration_sec;
  auto start = std::chrono::steady_clock::now();
  for(size_t i=0; i<request_times; i++) {
    TestCasePair pair{};
    if (!reader.pop(pair)) {
      BOOST_LOG_TRIVIAL(error)  << "pop data failed";
      break;
    }
    if (!bench.Request(i, pair.req, pair.response)) {
      BOOST_LOG_TRIVIAL(error)  << "request data failed";
      break;
    }
  }
  bench.WaitAllUntilTimeout(cfg.test_duration_sec * 1000);
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

  auto summary = bench.Summary();
  qps = summary.success_request_count / (elapsedtime_ms / 1000);
  dump_summary(summary, qps);
  return summary;
}

SearchServiceGprcBenchmark::SummaryData TestServiceStabilityScore(std::vector<std::string> services, TestCaseReader& reader, const TestStabilityConfig& cfg) {
  auto qps_limit = cfg.load_percent * cfg.max_qps;
  auto clis = SearchServiceGprcClient::CreateClients(services, qps_limit);
  if (clis.size() == 0) return {};
  auto comparator = std::make_shared<SearchServiceComparatorSample>(cfg.sample_percent_th, cfg.sample_score_th);
  SearchServiceGprcBenchmark bench(clis, comparator, cfg.timeout_ms, qps_limit);
  
  int32_t request_times = qps_limit * cfg.test_duration_sec;
  auto start = std::chrono::steady_clock::now();
  for(size_t i=0; i<request_times; i++) {
    TestCasePair pair{};
    if (!reader.pop(pair)) {
      BOOST_LOG_TRIVIAL(error)  << "pop data failed";
      break;
    }
    if (!bench.Request(i, pair.req, pair.response)) {
      BOOST_LOG_TRIVIAL(error)  << "request data failed";
      break;
    }
  }
  bench.WaitAllUntilTimeout(cfg.test_duration_sec * 1000);
  auto end = std::chrono::steady_clock::now();
  auto elapsedtime_ms = std::chrono::duration<double, std::milli>(end - start).count();

  auto summary = bench.Summary();
  auto qps = summary.success_request_count / (elapsedtime_ms / 1000);
  dump_summary(summary, qps);
  return summary;
}
