#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <unordered_map>
#include <limits>

#include <cmath>
#include <etcd/Client.hpp>
#include <boost/lockfree/queue.hpp>
#include <grpcpp/grpcpp.h>

#include "grpc_benchmark.h"
#include "test_case_reader.h"
#include "test_case_reader_preload.h"
#include "test_case_reader_async.h"
#include "test_search_service.h"
#include "config.h"
#include "alimama.grpc.pb.h"

#include "defines.h"

TestConfig g_config;

bool isLess(double a, double b, double tolerance=std::numeric_limits<float>::epsilon()) {
    return (!(std::abs(a - b) <= tolerance) && a < b);
}

std::vector<std::string> setupSearchService() {
  std::vector<std::string> services{};
  etcd::SyncClient etcd("http://etcd:2379");
  const std::string kPublicEndpoint = "/services/searchservice";

  bool found {false};
  std::string server_address {};

  std::chrono::seconds timeout(g_config.loading_time_sec_timeout);
  std::chrono::seconds interval(5);
  std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();

  // wait until timeout
  while (std::chrono::system_clock::now() - start < timeout) {
    auto resp = etcd.get(kPublicEndpoint);
    if (!resp.is_ok()) {
      BOOST_LOG_TRIVIAL(info) << "get key fail. key: " << kPublicEndpoint << ", err: " << resp.error_message();
      std::this_thread::sleep_for(interval);
      continue;
    }
    found = true;
    server_address = resp.value().as_string();
    break;
  }
  if (!found) return services;
  std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();
  std::chrono::duration<double> diff = end - start;
  BOOST_LOG_TRIVIAL(info) << "etcd connected successful. use " << diff.count() << " second";

  for (size_t i = 0; i < 1; i++) {
    BOOST_LOG_TRIVIAL(info)  << "found server_address " << server_address;
    services.push_back(server_address);
  }
  return services;
}

void Command(std::string command) {
  BOOST_LOG_TRIVIAL(info)  << "command: " << command;
	system(command.c_str());
}

struct Statistic { //用于最后评分的统计信息
  double result_score; // （结果正确性得分项）判分规则：ad集合全部正确加50分，相对顺序正确加30分，扣费（bid_prices）加20分； 备注：随机抽样判定500个请求即可以，验证期间超时时间设定为500ms；
  double max_qps;
  double capacity_score;  // （最大吞吐得分项）判分规则：假设最大吞吐为Xqps（最大超时时间100ms下超时率<0.1%），本项得分= 100*2X/3M，其中M为benchark的QPS数，超过1.5倍的M，则得分100；
  double p99_latency_ms;
  double response_time_score; // （响应时间得分项）判分规则：假设P99响应时间为T（ms），本项得分= （100-T)^2 / 100，超过100ms不得分;  备注：在上述最大吞吐条件下，同时此处的100ms根据主办方baseline实际给出
  double service_score; //（服务稳定性得分项）判分规则：以交付系统最大服务容量的60%压力（包括rainy case）压测5分钟，服务出现异常得0分，正常服务得100分；
  double final_score; // 评分逻辑：Score= 0.5 * ResultScore +  0.2 * ResponseTimeScore + 0.2 * CapacityScore + 0.1 * ServiceScore 
  double M = 1000; // M为benchark的QPS数
};

// // WRONG_ANSWER(4, "答案错误", "您提交的程序没有通过所有的测试用例"),
// // ACCEPTED(5, "答案正确", "恭喜！您提交的程序通过了所有的测试用例"),
// // TIME_LIMIT_EXCEEDED(6, "运行超时", "您的程序未能在规定时间内运行结束，请检查是否循环有错或算法复杂度过大。"),
// // EXECUTION_COMPLETED(28, "运行成功", "代码运行成功"),
void DumpStats(Statistic& stat) {
  json j;
  j["code"] = 28;
  j["extras"]["result_score"] = stat.result_score;
  j["extras"]["response_time_score"] = stat.response_time_score;
  j["extras"]["capacity_score"] = stat.capacity_score;
  j["extras"]["service_score"] = stat.service_score;
  j["extras"]["final_score"] = stat.final_score;
  j["extras"]["p99_latency_ms"] = stat.p99_latency_ms;
  j["extras"]["max_qps"] = stat.max_qps;
  j["extras"]["M"] = stat.M; // TODO: 待定项
  j["score"] = j["extras"]["score"]  = stat.final_score;
  std::ofstream of("./testbenchResult.json", std::ios::out);
  of << j.dump();
  of.close();
  BOOST_LOG_TRIVIAL(info)  << "得分情况：: " << j.dump();
}

bool TestResulCalcStat(std::vector<std::string> services, Statistic& stat) {
    BOOST_LOG_TRIVIAL(trace)  << "reader start ";
    auto& cfg = g_config.result_cfg;
    auto reader = TestCaseReaderPreload(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    SearchServiceGprcBenchmark::SummaryData summary{};
    BOOST_LOG_TRIVIAL(info) << "TestResultScore ";
    TestResultScore(services, reader, cfg, summary);
    reader.stop();

    auto& user_summary = summary.custom_summary;
    double avg_score = user_summary.total_score / user_summary.total_num;
    stat.result_score = avg_score;
    BOOST_LOG_TRIVIAL(info) << "result score:  " << avg_score;
    if (isLess(avg_score, cfg.final_score_th)) {
      return false;
    }
    return true;
}

bool TestMaxQpsCalcStat(std::vector<std::string> services, Statistic& stat) {
    auto& cfg = g_config.max_qps_cfg;
    auto reader = TestCaseReaderPreload(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    BOOST_LOG_TRIVIAL(info) << "TestMaxQps ";
    double max_qps = 0;
    auto summary = TestMaxQps(services, reader, cfg, max_qps);
    BOOST_LOG_TRIVIAL(info)  << "max_qps " << max_qps;
    stat.max_qps = max_qps;
    
    reader.stop();
    return true;
}

bool TestCapacityCalcStat(std::vector<std::string> services, Statistic& stat) {
    auto& cfg = g_config.capacity_cfg;
    auto reader = TestCaseReaderPreload(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    BOOST_LOG_TRIVIAL(info) << "TestCapacityScore ";
    auto summary = TestCapacityScore(services, reader, cfg);
    reader.stop();
    stat.M = cfg.M;
    if (summary.interupted || isLess(summary.success_request_percent, cfg.success_percent_th)) {
      stat.max_qps = 0;
      stat.capacity_score = 0;
      BOOST_LOG_TRIVIAL(info) << "capacity score " << stat.capacity_score;
      return false;
    }
    stat.max_qps = summary.custom_summary.qps;
    stat.capacity_score = (100 * 2 * stat.max_qps) / (3 * cfg.M);
    if (stat.capacity_score > 100) stat.capacity_score = 100;
    BOOST_LOG_TRIVIAL(info) << "capacity score " << stat.capacity_score;
    return true;
}

bool TestResponseTimeCalcStat(std::vector<std::string> services, int32_t max_qps, Statistic& stat) {
    auto& cfg = g_config.response_time_cfg;
    auto reader = TestCaseReaderPreload(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    cfg.max_qps = max_qps;
    BOOST_LOG_TRIVIAL(info) << "TestResponseTime ";
    double qps = 0;
    auto summary = TestResponseTime(services, reader, cfg, qps);
    reader.stop();
    if (summary.interupted || isLess(summary.success_request_percent, cfg.success_percent_th)) {
      stat.response_time_score = 0;
      BOOST_LOG_TRIVIAL(info)  << "qps " << qps << " max_qps " << max_qps << " response_time_score " << stat.response_time_score;
      return false;
    }
    stat.p99_latency_ms = summary.p99_latency_ms;
    stat.response_time_score = stat.p99_latency_ms < cfg.timeout_ms ?
      std::pow((cfg.timeout_ms - summary.p99_latency_ms), 2) / 100 : 0;
    BOOST_LOG_TRIVIAL(info) << "stat.p99_latency_ms " << stat.p99_latency_ms << " cfg.timeout_ms " << cfg.timeout_ms
      << " qps " << qps << " max_qps " << max_qps << " response_time_score " << stat.response_time_score;
    return true;
}

bool TestServiceStabilityCalcStat(std::vector<std::string> services, int32_t max_qps, Statistic& stat) {
    auto& cfg = g_config.stability_cfg;
    auto reader = TestCaseReaderPreload(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    cfg.max_qps = max_qps;
    BOOST_LOG_TRIVIAL(info) << "TestServiceStabilityScore ";
    auto summary = TestServiceStabilityScore(services, reader, cfg);
    reader.stop();
    if (summary.interupted || isLess(summary.success_request_percent, cfg.success_percent_th)) {
      stat.service_score = 0;
      BOOST_LOG_TRIVIAL(info) << "service score " << stat.service_score;
      return false;
    }
    stat.service_score = 100;
    BOOST_LOG_TRIVIAL(info) << "service score " << stat.service_score;
    return true;
}

bool CheckStubs(StubsVector& stubs) {
  if (stubs.size() == 0) {
    BOOST_LOG_TRIVIAL(error)  << "failed to setup serach service ";
  }
  for(auto& stub: stubs) {
    if (!stub) {
      BOOST_LOG_TRIVIAL(error)  << "failed to setup serach service , got nullptr ";
      return false;
    }
  }
  return true;
}

void TestAll(Statistic& stat) {
  BOOST_LOG_TRIVIAL(trace)  << "setup service";
  auto services = setupSearchService();
  if (services.size() == 0) return;

  if (!TestResulCalcStat(services, stat)) return;
  // if (!TestMaxQpsCalcStat(services, stat)) return;
  if (!TestCapacityCalcStat(services, stat)) return;
  if (!TestResponseTimeCalcStat(services, stat.max_qps, stat)) return;
  if (!TestServiceStabilityCalcStat(services, stat.max_qps, stat)) return;
}

void init_logging() {
  logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);
}

int main(int argc, char** argv) {
  init_logging();

  const std::string kConfigFilePath("config.json");
  bool ok = ReadConfigFromFile(kConfigFilePath, g_config);
  if (!ok) {
    return -1;
  }

  Statistic stat{};
  TestAll(stat);
  stat.final_score = 0.5 * stat.result_score + 0.2 * stat.response_time_score + 0.2 * stat.capacity_score + 0.1 * stat.service_score;
  DumpStats(stat);
  return 0;
}
