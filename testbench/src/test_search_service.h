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

#define BOOST_LOG_DYN_LINK 1
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
namespace logging = boost::log;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using RequestPtr = std::shared_ptr<alimama::proto::Request>;
using ResponsePtr = std::shared_ptr<alimama::proto::Response>;
using alimama::proto::SearchService;
using alimama::proto::Request;
using alimama::proto::Response;

struct CustomSummary{
  int32_t total_num;
  int32_t ad_correct_num; // 集合和顺序都正确
  int32_t ad_partial_correct_num; // 集合正确，顺序不正确
  int32_t price_correct_num; // 价格正确
};
using StubsVector=std::vector<std::unique_ptr<SearchService::Stub>>;
using SearchServiceGprcBenchmark = GrpcBenchmark<RequestPtr, ResponsePtr, CustomSummary, ResponsePtr>;
using GrpcClientPtr = shared_ptr<GrpcClient<RequestPtr, ResponsePtr>>;

struct RequestItem {
  std::shared_ptr<ClientContext> ctx;
  void* obj;
  RequestPtr req;
  ResponsePtr resp;
  std::shared_ptr<Status> status;
};

class SearchServiceGprcClient : public GrpcClient<RequestPtr, ResponsePtr> {
private:
  StubsVector stubs_;
  std::vector<std::string> services_;
  shared_ptr<grpc::CompletionQueue> cq_;
  std::atomic<uint64_t> req_idx_;

  std::mutex mtx_;
  bool enable_;
public:
  static std::vector<shared_ptr<GrpcClient<RequestPtr, ResponsePtr>>> CreateClients(const std::vector<std::string>& services, int32_t qps_limit) {
    std::vector<shared_ptr<GrpcClient<RequestPtr, ResponsePtr>>> clients{};
    constexpr int32_t kDefaultQpsEachThread{4000};
    auto cli_num = (qps_limit / kDefaultQpsEachThread) + 1;
    for(size_t i=0; i<cli_num; i++) {
      auto cli = std::make_shared<SearchServiceGprcClient>(services);
      if (!cli->Init()) {
        BOOST_LOG_TRIVIAL(error)  << "init clients failed";
        return std::vector<shared_ptr<GrpcClient<RequestPtr, ResponsePtr>>>{};
      }
      clients.push_back(cli);
    }
    return clients;
  };

  SearchServiceGprcClient(const std::vector<std::string> services):
    services_(services), req_idx_{0},enable_{false},cq_() {};
  ~SearchServiceGprcClient() {
  };

  bool Init() {
    for (const auto& svc : services_) {
      std::unique_ptr<SearchService::Stub> stub(SearchService::NewStub(grpc::CreateChannel(svc, grpc::InsecureChannelCredentials())));
      if (!stub) {
        BOOST_LOG_TRIVIAL(error)  << "failed to setup serach service , got nullptr ";
        return false;
      }
      stubs_.push_back(std::move(stub));
      auto q = std::make_shared<grpc::CompletionQueue>();
      cq_.swap(q);
    }
    enable_ = true;
    return true;
  }

  bool Request(std::shared_ptr<ClientContext> ctx, RequestPtr& req, void* obj) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!enable_) return false;
    auto idx = req_idx_.fetch_add(1);
    idx = idx % stubs_.size();
    std::unique_ptr<grpc::ClientAsyncResponseReader<Response> > rpc(
      stubs_[idx]->AsyncSearch(ctx.get(), *req, cq_.get()));
    ResponsePtr resp = std::make_shared<Response>();
    auto status = std::make_shared<Status>();
    auto* item = new RequestItem{ctx, obj, req, resp, status};
    rpc->Finish(resp.get(), status.get(), (void*)item);
    return true;
  }

  bool WaitResponse(ResponsePtr& resp, std::shared_ptr<Status>& status, void** obj) {
    void * got;
    bool ok = false;
    bool success = cq_->Next(&got, &ok);
    if (!success) {
      return false;
    }
    if (!ok) {
      BOOST_LOG_TRIVIAL(warning) << "request !OK";
      // TODO
      return false;
    }
    RequestItem* got_item = (RequestItem*) got;
    if (!got_item) {
      BOOST_LOG_TRIVIAL(warning) << "request !got_item";
      return false;
    }
    if (obj) {
      *obj = got_item->obj;
    }
    resp.swap(got_item->resp);
    status.swap(got_item->status);
    delete got_item;
    return true;
  }
  bool Close() {
    std::lock_guard<std::mutex> lock(mtx_);
    cq_->Shutdown();
    enable_ = false;
    return true;
  }

  SearchServiceGprcClient(const SearchServiceGprcClient&) = delete;
  SearchServiceGprcClient(SearchServiceGprcClient&&) = delete;
  SearchServiceGprcClient& operator=(const SearchServiceGprcClient&) = delete;
  SearchServiceGprcClient& operator=(SearchServiceGprcClient&&) = delete;
};

void dump_summary(const SearchServiceGprcBenchmark::SummaryType& summary, double qps) {
  BOOST_LOG_TRIVIAL(info)  << "summary completed_requests " << summary.completed_requests;
  BOOST_LOG_TRIVIAL(info)  << "summary avg_latency_ms " << summary.avg_latency_ms;
  BOOST_LOG_TRIVIAL(info)  << "summary p99_latency_ms " << summary.p99_latency_ms;
  BOOST_LOG_TRIVIAL(info)  << "summary qps " << qps;
  BOOST_LOG_TRIVIAL(info)  << "summary error_request_count " << summary.error_request_count;
  BOOST_LOG_TRIVIAL(info)  << "summary success_request_count " << summary.success_request_count;
  BOOST_LOG_TRIVIAL(info)  << "summary success_request_percent " << summary.success_request_percent;
  BOOST_LOG_TRIVIAL(info)  << "summary timeout_request_count " << summary.timeout_request_count;
}

bool compare_result_dummy(const ResponsePtr& resp, const ResponsePtr& ref, CustomSummary& result) {
  return true;
}

bool compare_result(const ResponsePtr& resp, const ResponsePtr& ref, CustomSummary& result) {
  if (!resp) {
    BOOST_LOG_TRIVIAL(warning)  << "resp is null ";
    return true;
  }
  if (!ref) {
    BOOST_LOG_TRIVIAL(warning)  << "ref is null ";
    return true;
  }
  // Check adgroup_ids
  if (ref->adgroup_ids().size() == 0 && resp->adgroup_ids().size() == 0) {
    result.ad_correct_num ++;
  } else if (ref->adgroup_ids().size() != resp->adgroup_ids().size()) {
  } else if (std::equal(resp->adgroup_ids().begin(), resp->adgroup_ids().end(), ref->adgroup_ids().begin(), ref->adgroup_ids().end())) {
    result.ad_correct_num ++;
  } else if (std::is_permutation(resp->adgroup_ids().begin(), resp->adgroup_ids().end(), ref->adgroup_ids().begin())) {
    result.ad_partial_correct_num ++;
  }

  // Check prices
  if (ref->prices().size() == 0 && resp->prices().size() == 0) {
    result.price_correct_num ++;
  } else if (ref->prices().size() != resp->prices().size()) {
  } else if (std::equal(resp->prices().begin(), resp->prices().end(), ref->prices().begin(), ref->prices().end())) {
    result.price_correct_num ++;
  }
  result.total_num ++;
  return true;
}

void TestResultScore(std::vector<std::string> services, TestCaseReader& reader,const TestResultConfig& cfg,
    SearchServiceGprcBenchmark::SummaryType& summary, double& qpsBaseLine) {
  auto clis = SearchServiceGprcClient::CreateClients(services, cfg.qps_limit);
  if (clis.size() == 0) return;
  SearchServiceGprcBenchmark bench(clis, compare_result, cfg.timeout_ms, cfg.qps_limit);
  auto start = std::chrono::steady_clock::now();
  BOOST_LOG_TRIVIAL(trace)  << "TestResultScore request num " << cfg.request_times;
  for(size_t i=0; i<cfg.request_times; i++) {
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
  qpsBaseLine = summary.success_request_count / (elapsedtime_ms / 1000);
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
SearchServiceGprcBenchmark::SummaryType TestMaxQps(std::vector<std::string> services, TestCaseReader& reader, const TestMaxQpsConfig& cfg,
    double& max_qps) {
  double qps_limit = cfg.qps_baseline > 0 ? cfg.qps_baseline : kDefaultQpsBaseline;
  double last_qps = 0;
  max_qps = 0;

  SearchServiceGprcBenchmark::SummaryType last_summary;
  for (size_t i = 0; i<cfg.max_iter_times; ++i) {
    auto clis = SearchServiceGprcClient::CreateClients(services, int32_t(qps_limit));
    if (clis.size() == 0) return last_summary;

    SearchServiceGprcBenchmark bench(clis, compare_result_dummy, cfg.timeout_ms, int32_t(qps_limit));
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

SearchServiceGprcBenchmark::SummaryType TestResponseTime(std::vector<std::string> services, TestCaseReader& reader, const TestResponseTimeConfig& cfg, double& qps) {
  auto qps_limit = cfg.max_qps;
  auto clis = SearchServiceGprcClient::CreateClients(services, qps_limit);
  if (clis.size() == 0) return {};
  SearchServiceGprcBenchmark bench(clis, compare_result_dummy, cfg.timeout_ms, qps_limit);
  
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
  return summary;
}

SearchServiceGprcBenchmark::SummaryType TestServiceStabilityScore(std::vector<std::string> services, TestCaseReader& reader, const TestStabilityConfig& cfg) {
  auto qps_limit = cfg.load_percent * cfg.max_qps;
  auto clis = SearchServiceGprcClient::CreateClients(services, qps_limit);
  if (clis.size() == 0) return {};
  SearchServiceGprcBenchmark bench(clis, compare_result_dummy, cfg.timeout_ms, qps_limit);
  
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
  return summary;
}
