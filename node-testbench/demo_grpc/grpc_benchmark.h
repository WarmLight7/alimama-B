#pragma once

#include <boost/lockfree/queue.hpp>
#include <thread>
#include <vector>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <boost/heap/priority_queue.hpp>

#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/client_context.h>
#include <grpc/grpc.h>
#include <grpc/support/time.h>

#define BOOST_LOG_DYN_LINK 1
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
namespace logging = boost::log;

#include "concurent_queue.h"

using grpc::Status;
using grpc::ClientContext;

template <typename T_Req>
struct EachReqInternal {
    uint64_t id;
    T_Req request;
};

template <typename T_Resp>
struct EachResp {
    uint64_t id;
    T_Resp response;
    Status status;
    double latency;
};

template <typename T_Resp>
struct SummaryData {
    uint64_t completed_requests;
    uint64_t success_request_count;
    double  success_request_percent;
    uint64_t error_request_count;
    uint64_t timeout_request_count;
    double avg_latency_ms;
    double p99_latency_ms;
    std::vector<EachResp<T_Resp>> data;
};

template <typename T_Req, typename T_Resp>
class GrpcBenchmark {
private:
    ConcurrentQueue<EachReqInternal<T_Req>> requests_;
    ConcurrentQueue<EachResp<T_Resp>> responses_;
    std::vector<std::thread> threads_;
    std::atomic<bool> enable_;
    std::atomic<int32_t> pending_num_;
    std::mutex mutex_;
    std::condition_variable cv_wait_;
    double max_timesleep_ms_;

    int64_t deadline_ms_;
public:
    using DoRequestFunc = std::function<Status(ClientContext&, T_Req&, T_Resp&)>;
    using SummaryType = SummaryData<T_Resp>;
    GrpcBenchmark(DoRequestFunc do_request, int32_t threads_num=1, int64_t deadline_ms_=10, int32_t qps_limit=1000):
        threads_(threads_num), enable_(true), pending_num_{0}, deadline_ms_{deadline_ms_},requests_(0, "bencharmk_requests"),responses_(0, "bencharmk_responses") {
        this->max_timesleep_ms_ = 1 * 1000/(qps_limit * 1.0 / threads_num);
        BOOST_LOG_TRIVIAL(info) << " threads_num " << threads_num << " deadline_ms_ " << deadline_ms_ << " qps_limit " << qps_limit << " max_timesleep_ms " << this->max_timesleep_ms_ << std::endl;

        for (size_t i = 0; i < this->threads_.size(); ++i) {
            threads_[i] = std::thread([&]() {
                EachReqInternal<T_Req> each;
                while (this->enable_.load() && this->requests_.WaitAndPop(each) != QueueStatus::Closed) {
                    ClientContext context;
                    auto start = std::chrono::steady_clock::now();
                    gpr_timespec timeout = gpr_time_from_millis(this->deadline_ms_, GPR_TIMESPAN);
                    context.set_deadline(timeout);
                    T_Resp resp;
                    Status status = do_request(context, each.request, resp);
                    
                    auto end = std::chrono::steady_clock::now();
                    auto latency = std::chrono::duration<double, std::milli>(end - start).count();
                    EachResp<T_Resp> eachResp{
                        each.id,
                        resp,
                        status,
                        latency
                    };
                    if (this->responses_.Push(eachResp) == QueueStatus::Closed) {
                        break;
                    }
                    this->pending_num_ --;
                    if (this->pending_num_ == 0) {
                        this->cv_wait_.notify_all();
                    }
                    auto sleep_ms = this->max_timesleep_ms_ - latency;
                    if(sleep_ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(int32_t(sleep_ms)));
                    }
                }
            });
        }
    }
    GrpcBenchmark(const GrpcBenchmark&) = delete;
    GrpcBenchmark(GrpcBenchmark&&) = delete;
    GrpcBenchmark& operator=(const GrpcBenchmark&) = delete;
    GrpcBenchmark& operator=(GrpcBenchmark&&) = delete;

    ~GrpcBenchmark() {
        this->enable_.store(false);
        this->requests_.Close();
        this->responses_.Close();
        this->cv_wait_.notify_all();
        for (size_t i = 0; i < this->threads_.size(); ++i) {
            this->threads_[i].join();
        }
    }

    bool Request(uint64_t id, const T_Req& req) {
        this->pending_num_ ++;
        EachReqInternal<T_Req> internal {
            id,
            req
        };
        if (this->requests_.Push(internal) == QueueStatus::Closed) {
            return false;
        } else {
            return true;
        }
    }

    void WaitAll() {
        while (this->pending_num_ > 0 && this->enable_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // return false if timeout
    bool WaitAllUntilTimeout(int64_t timeout_ms=0) {
        std::unique_lock<std::mutex> lock(this->mutex_);

        if (this->cv_wait_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&] { return !this->enable_.load() || (this->enable_.load() && this->pending_num_.load() == 0); })) {
            return true;
        } else { // timeout
            this->enable_.store(false);
            this->requests_.Close();
            this->responses_.Close();
            return false;
        }
    }

    SummaryData<T_Resp> Summary() {
        SummaryData<T_Resp> resps{};
        EachResp<T_Resp> resp;
        double total_latency = 0;
        boost::heap::priority_queue<double> pq{}; 
        while(true) {
            auto status = this->responses_.TryPop(resp);
            if (status == QueueStatus::Empty || status == QueueStatus::Closed) {
                break;
            }
            resps.completed_requests ++;
            if (!resp.status.ok() && resp.status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
                resps.timeout_request_count ++;
            } else if (!resp.status.ok()) {
                resps.error_request_count ++;
            } else {
                total_latency += resp.latency;
                resps.success_request_count ++;
                pq.push(resp.latency);
            }
            resps.data.push_back(resp);
        }
        resps.avg_latency_ms = total_latency / resps.success_request_count;
        int32_t p99_index = int32_t(resps.success_request_count * 0.01);
        boost::heap::priority_queue<double>::const_iterator it = pq.begin(); 
        std::advance(it, p99_index);
        if (it != pq.end()) {
            resps.p99_latency_ms = *it;
        }
        resps.success_request_percent = resps.success_request_count * 1.0 / resps.completed_requests;
        return resps;
    }
};
