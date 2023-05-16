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

template <typename T_Req,typename T_Resp>
struct EachReqInternal {
    uint64_t id;
    T_Req request;
    T_Resp ref_resp;
};

template <typename T_Resp>
struct EachResp {
    uint64_t id;
    T_Resp response;
    T_Resp ref_resp;
    Status status;
    double latency;
};

template <typename T_Resp_Summary>
struct SummaryData {
    uint64_t completed_requests;
    uint64_t success_request_count;
    double  success_request_percent;
    uint64_t error_request_count;
    uint64_t timeout_request_count;
    double avg_latency_ms;
    double p99_latency_ms;
    T_Resp_Summary user_summary;
};

template <typename T_Req, typename T_Resp, typename T_Resp_Summary>
class GrpcBenchmark {
private:
    ConcurrentQueue<EachReqInternal<T_Req, T_Resp>> requests_;
    ConcurrentQueue<EachResp<T_Resp>> responses_;
    std::vector<std::thread> threads_;
    std::thread collector_;
    std::atomic<bool> enable_;
    std::atomic<int32_t> pending_num_;
    std::mutex mutex_;
    std::condition_variable cv_wait_;
    double max_timesleep_ms_;
    SummaryData<T_Resp_Summary> summary_data_;
    double total_latency_;
    boost::heap::priority_queue<double> pq_;

    int64_t deadline_ms_;

    void updateSummary(const EachResp<T_Resp>& resp) {
        this->summary_data_.completed_requests ++;
        if (!resp.status.ok() && resp.status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
            this->summary_data_.timeout_request_count ++;
        } else if (!resp.status.ok()) {
            this->summary_data_.error_request_count ++;
        } else {
            this->total_latency_ += resp.latency;
            this->summary_data_.success_request_count ++;
            this->pq_.push(resp.latency);
        }
    }
public:
    using DoRequestFunc = std::function<Status(ClientContext&, T_Req&, T_Resp&)>;
    using DoCompareFunc = std::function<bool(const T_Resp&, const T_Resp&, T_Resp_Summary&)>;
    using SummaryType = SummaryData<T_Resp_Summary>;
    GrpcBenchmark(DoRequestFunc do_request, DoCompareFunc do_compare, int32_t threads_num=1, int64_t deadline_ms_=10, int32_t qps_limit=1000):
        threads_(threads_num), enable_(true), pending_num_{0}, deadline_ms_{deadline_ms_},requests_(0, "bencharmk_requests"),responses_(0, "bencharmk_responses"),
        summary_data_{} {
        this->max_timesleep_ms_ = 1 * 1000/(qps_limit * 1.0 / threads_num);
        BOOST_LOG_TRIVIAL(info) << " threads_num " << threads_num << " deadline_ms_ " << deadline_ms_ << " qps_limit " << qps_limit << " max_timesleep_ms " << this->max_timesleep_ms_ << std::endl;

        for (size_t i = 0; i < this->threads_.size(); ++i) {
            threads_[i] = std::thread([this,do_request]() {
                EachReqInternal<T_Req, T_Resp> each;
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
                        each.ref_resp,
                        status,
                        latency
                    };
                    if (this->responses_.Push(eachResp) == QueueStatus::Closed) {
                        break;
                    }

                    auto sleep_ms = this->max_timesleep_ms_ - latency;
                    if(sleep_ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(int32_t(sleep_ms)));
                    }
                }
            });
        }

        this->collector_ = std::thread([this, do_compare]() {
            while(this->enable_.load()) {
                EachResp<T_Resp> resp{};
                auto status = this->responses_.WaitAndPop(resp);
                if (status == QueueStatus::Closed) {
                    break;
                }
                this->updateSummary(resp);
                auto ok = do_compare(resp.response, resp.ref_resp, this->summary_data_.user_summary);
                this->pending_num_ --;
                if (this->pending_num_ == 0) {
                    this->cv_wait_.notify_all();
                }
            }
        });
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
        this->collector_.join();
    }

    bool Request(uint64_t id, const T_Req& req, const T_Resp& ref_resp) {
        this->pending_num_ ++;
        EachReqInternal<T_Req, T_Resp> internal {
            id,
            req,
            ref_resp
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
        this->enable_.store(false);
        this->requests_.Close();
        this->responses_.Close();
    }

    // return false if timeout
    bool WaitAllUntilTimeout(int64_t timeout_ms=0) {
        std::unique_lock<std::mutex> lock(this->mutex_);

        if (this->cv_wait_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return !this->enable_.load() || (this->enable_.load() && this->pending_num_.load() == 0); })) {
            return true;
        } else { // timeout
            this->enable_.store(false);
            this->requests_.Close();
            this->responses_.Close();
            return false;
        }
    }

    SummaryType Summary() {
        this->summary_data_.avg_latency_ms = this->total_latency_ / this->summary_data_.success_request_count;
        int32_t p99_index = int32_t(this->summary_data_.success_request_count * 0.01);
        boost::heap::priority_queue<double>::const_iterator it = this->pq_.begin(); 
        std::advance(it, p99_index);
        if (it != this->pq_.end()) {
            this->summary_data_.p99_latency_ms = *it;
        }
        this->summary_data_.success_request_percent = this->summary_data_.success_request_count * 1.0 / this->summary_data_.completed_requests;
        return this->summary_data_;
    }
};
