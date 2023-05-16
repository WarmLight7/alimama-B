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

#include "grpc_benchmark.h"
#include "test_case_reader.h"
#include "test_case_reader_preload.h"
#include "test_case_reader_async.h"
#include "config.h"
#include "test_search_service.h"
#include "alimama.grpc.pb.h"

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

using StubsVector=std::vector<std::unique_ptr<SearchService::Stub>>;
StubsVector setupSearchService() {
  StubsVector stubs{};
  etcd::Client etcd("http://etcd:2379");
  std::string prefix = "/services/searchservice/";
	etcd::Response response = etcd.keys(prefix).get();
  if (response.is_ok()) {
      BOOST_LOG_TRIVIAL(info) << "etcd connected successful.";
  } else {
      BOOST_LOG_TRIVIAL(info) <<  "etcd connected failed: " << response.error_message();
      return stubs;
  }

  for (size_t i = 0; i < response.keys().size(); i++) {
    std::string server_address = std::string(response.key(i)).substr(prefix.size());
    BOOST_LOG_TRIVIAL(info)  << "found server_address " << server_address;
    std::unique_ptr<SearchService::Stub> stub(SearchService::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())));
    stubs.push_back(std::move(stub));
  }
  return stubs;
}

void Command(std::string command) {
  BOOST_LOG_TRIVIAL(info)  << "command: " << command;
	system(command.c_str());
}

struct Statistic { //用于最后评分的统计信息
  double result_score; // （结果正确性得分项）判分规则：ad集合全部正确加50分，相对顺序正确加30分，扣费（bid_prices）加20分； 备注：随机抽样判定500个请求即可以，验证期间超时时间设定为500ms；
  double response_time_score; // （响应时间得分项）判分规则：假设P99响应时间为T（ms），本项得分= （100-T)^2 / 100，超过100ms不得分;  备注：在上述最大吞吐条件下，同时此处的100ms根据主办方baseline实际给出
  double capacity_score;  // （最大吞吐得分项）判分规则：假设最大吞吐为Xqps（最大超时时间100ms下超时率<0.1%），本项得分= 100*2X/3M，其中M为benchark的QPS数，超过1.5倍的M，则得分100；
  double service_score; //（服务稳定性得分项）判分规则：以交付系统最大服务容量的60%压力（包括rainy case）压测5分钟，服务出现异常得0分，正常服务得100分；
  double final_score; // 评分逻辑：Score= 0.5 * ResultScore +  0.2 * ResponseTimeScore + 0.2 * CapacityScore + 0.1 * ServiceScore 
  double p99_latency_ms;
  double max_qps;
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
  of << j.dump() << std::endl;
  of.close();
  BOOST_LOG_TRIVIAL(info)  << "得分情况：: " << j.dump();
}

double TestResulCalcStat(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestResultConfig& cfg, Statistic& stat) {
    BOOST_LOG_TRIVIAL(trace)  << "reader start " << std::endl;
    auto reader = TestCaseReaderAsync(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    SearchServiceGprcBenchmark::SummaryType summary{};
    double qps_baseline {};
    BOOST_LOG_TRIVIAL(trace)  << "TestResultScore " << std::endl;
    TestResultScore(doreqeust, reader, cfg, summary, qps_baseline);
    BOOST_LOG_TRIVIAL(info)  << "qps_baseline " << qps_baseline << std::endl;
    auto& user_summary = summary.user_summary;
    if (user_summary.ad_correct_num == user_summary.total_num) {
      stat.result_score += 80;
    } else if (user_summary.ad_partial_correct_num == summary.user_summary.total_num) {
      stat.result_score += 50;
    } else if ((user_summary.ad_partial_correct_num + user_summary.ad_correct_num) == user_summary.total_num) {
      stat.result_score += 50;
    }
    if (user_summary.price_correct_num == user_summary.total_num) {
      stat.result_score += 20;
    }
    reader.stop();
    return qps_baseline;
}

double TestMaxQpsCalcStat(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestMaxQpsConfig& cfg, int32_t qps_baseline, Statistic& stat) {
    auto reader = TestCaseReaderAsync(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    cfg.qps_baseline = qps_baseline;
    BOOST_LOG_TRIVIAL(trace)  << "TestMaxQps " << std::endl;
    double max_qps = 0;
    auto summary = TestMaxQps(doreqeust, reader, cfg, max_qps);
    BOOST_LOG_TRIVIAL(info)  << "max_qps " << max_qps << std::endl;
    stat.max_qps = max_qps;
    
    reader.stop();
    return max_qps;
}

void TestServiceStabilityCalcStat(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestStabilityConfig& cfg, int32_t max_qps, Statistic& stat) {
    auto reader = TestCaseReaderAsync(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    cfg.max_qps = max_qps;
    BOOST_LOG_TRIVIAL(trace)  << "TestServiceStabilityScore " << std::endl;
    auto summary = TestServiceStabilityScore(doreqeust, reader, cfg);
    if (summary.completed_requests == summary.success_request_count) {
      stat.service_score = 100;
    } else {
      stat.service_score = 0;
    }
    reader.stop();
}

void TestResponseTimeCalcStat(SearchServiceGprcBenchmark::DoRequestFunc doreqeust, TestResponseTimeConfig& cfg, int32_t max_qps, Statistic& stat) {
    auto reader = TestCaseReaderAsync(cfg.test_case_csv, cfg.csv_reader_capacity);
    reader.start();

    cfg.max_qps = max_qps;
    BOOST_LOG_TRIVIAL(trace)  << "TestResponseTime " << std::endl;
    double qps = 0;
    auto summary = TestResponseTime(doreqeust, reader, cfg, qps);
    if (summary.success_request_percent < 0.99) {
      stat.response_time_score = 0;
    } else {
      stat.response_time_score = std::pow((cfg.timeout_ms - summary.p99_latency_ms), 2) / 100;
      stat.p99_latency_ms = summary.p99_latency_ms;
      stat.capacity_score = 100 * 2 * qps / 3 * max_qps;
    }

    reader.stop();
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

const std::string kConfigFilePath("config.json");
void TestAll(Statistic& stat) {
  auto stubs = setupSearchService();
  auto ok = CheckStubs(stubs);
  if (!ok) {
    BOOST_LOG_TRIVIAL(error)  << "check stubs failed ";
    return;
  }

  std::atomic<uint32_t> round_robin_idx{0};
  SearchServiceGprcBenchmark::DoRequestFunc doreqeust = [&stubs, &round_robin_idx](ClientContext& ctx, Request& req, Response &resp) -> Status {
    auto idx = round_robin_idx.fetch_add(1);
    idx = idx % stubs.size();
    return stubs[idx]->Search(&ctx, req, &resp);
  };

  TestResultConfig test_result_cfg {};
  TestMaxQpsConfig test_max_qps_cfg {};
  TestStabilityConfig test_stability_cfg {};
  TestResponseTimeConfig test_response_time_cfg {};
  ReadConfigFromFile(kConfigFilePath, test_result_cfg, test_max_qps_cfg, test_stability_cfg, test_response_time_cfg);
  auto qps_baseline = TestResulCalcStat(doreqeust, test_result_cfg, stat);
  auto max_qps = TestMaxQpsCalcStat(doreqeust, test_max_qps_cfg, qps_baseline, stat);
  TestResponseTimeCalcStat(doreqeust, test_response_time_cfg, max_qps, stat);
  TestServiceStabilityCalcStat(doreqeust, test_stability_cfg, max_qps, stat);
  stat.final_score = 0.5 * stat.result_score + 0.2 * stat.response_time_score + 0.2 * stat.capacity_score + 0.1 * stat.service_score;
}

void init_logging() {
  logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);
}

int main(int argc, char** argv) {
  init_logging();
  BOOST_LOG_TRIVIAL(trace)  << "setup service" << std::endl;

  Statistic stat{};
  TestAll(stat);

  DumpStats(stat);
  return 0;
}
