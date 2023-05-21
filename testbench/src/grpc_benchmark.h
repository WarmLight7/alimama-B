#pragma once

#include <boost/lockfree/queue.hpp>
#include <thread>
#include <vector>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <memory>
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
using std::shared_ptr;

template <typename T_Req,typename T_Resp>
struct EachReqInternal {
    uint64_t id;
    T_Req request;
    T_Resp ref_resp;
};

template <typename T_Resp>
struct EachResp {
    uint64_t id;
    shared_ptr<std::chrono::steady_clock::time_point> start_t;
    T_Resp response;
    T_Resp ref_resp;
    shared_ptr<Status> status;
    double latency;
};

template <typename T_Cusom_Summary>
struct SummaryData {
    uint64_t completed_requests;
    uint64_t success_request_count;
    double  success_request_percent;
    uint64_t error_request_count;
    uint64_t timeout_request_count;
    double avg_latency_ms;
    double p99_latency_ms;
    T_Cusom_Summary custom_summary;
};

using grpc::CompletionQueue;

template <typename T_Req, typename T_Resp>
class GrpcClient {
public:
    virtual ~GrpcClient() {}
    virtual bool Init() = 0;
    virtual bool Request(shared_ptr<ClientContext> ctx, T_Req& req, void* obj) = 0;
    virtual bool WaitResponse(T_Resp& resp, shared_ptr<Status>& status, void** obj) = 0;
    virtual bool Close() = 0;
};

template <typename T_Req, typename T_Resp, typename T_Cusom_Summary>
class GrpcBenchmark {
public:
    using DoRequestFunc = std::function<Status(ClientContext&, T_Req&, T_Resp&)>;
    using DoCompareFunc = std::function<bool(const T_Resp&, const T_Resp&, T_Cusom_Summary&)>;
    using SummaryType = SummaryData<T_Cusom_Summary>;
private:
    shared_ptr<GrpcClient<T_Req, T_Resp>> cli_;
    ConcurrentQueue<EachReqInternal<T_Req, T_Resp>> requests_;
    ConcurrentQueue<EachResp<T_Resp>> responses_;
    std::vector<std::thread> threads_;
    
    std::thread calculator_;
    std::thread collector_;
    std::atomic<bool> enable_;
    std::atomic<int32_t> pending_num_;
    std::mutex mutex_;
    std::condition_variable cv_wait_;

    std::atomic<int32_t> pending_compare_num_;
    std::mutex summary_mutex_;
    std::condition_variable summary_cv_wait_;

    double max_timesleep_ms_;
    SummaryData<T_Cusom_Summary> summary_data_;
    double total_latency_;
    boost::heap::priority_queue<double> pq_;

    int64_t deadline_ms_;

    bool updateSummary(const EachResp<T_Resp>& resp) {
        CompletionQueue cq;
        
        this->summary_data_.completed_requests ++;
        if (!resp.status->ok() && resp.status->error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
            this->summary_data_.timeout_request_count ++;
            return false;
        } else if (!resp.status->ok()) {
            this->summary_data_.error_request_count ++;
            return false;
        } else {
            this->total_latency_ += resp.latency;
            this->summary_data_.success_request_count ++;
            this->pq_.push(resp.latency);
            return true;
        }
        return true;
    }
public:
    GrpcBenchmark(shared_ptr<GrpcClient<T_Req, T_Resp>> cli, DoCompareFunc do_compare, int32_t threads_num=1, int64_t deadline_ms_=10, int32_t qps_limit=1000):
            cli_{cli}, threads_(threads_num), enable_(true), pending_num_{0}, pending_compare_num_{0},
            deadline_ms_{deadline_ms_},requests_(0, "bencharmk_requests"),responses_(0, "bencharmk_responses"),
        summary_data_{} {
        this->max_timesleep_ms_ = 1 * 1000/(qps_limit * 1.0 / threads_num);
        BOOST_LOG_TRIVIAL(info) << " threads_num " << threads_num << " deadline_ms_ " << deadline_ms_ << " qps_limit " << qps_limit << " max_timesleep_ms " << this->max_timesleep_ms_ << std::endl;

        for (size_t i = 0; i < this->threads_.size(); ++i) {
            threads_[i] = std::thread([this]() {
                EachReqInternal<T_Req, T_Resp> each;
                while (this->enable_.load()) {
                    auto start = std::chrono::steady_clock::now();
                    auto status = this->requests_.WaitAndPop(each);
                    if(status == QueueStatus::Closed)
                        break;
                    auto end_pop = std::chrono::steady_clock::now();
                    
                    auto ctx = std::make_shared<ClientContext>();
                    gpr_timespec timeout = gpr_time_from_millis(this->deadline_ms_, GPR_TIMESPAN);
                    ctx->set_deadline(timeout);

                    auto* eachResp = new EachResp<T_Resp>{};
                    eachResp->id = each.id;
                    eachResp->ref_resp = each.ref_resp;
                    eachResp->start_t = std::make_shared<std::chrono::steady_clock::time_point>(start);
                    bool ok = this->cli_->Request(ctx, each.request, eachResp);
                    if (!ok) {
                        continue;
                    }
                    auto end = std::chrono::steady_clock::now();
                    auto latency = std::chrono::duration<double, std::milli>(end - start).count();
                    auto sleep_ms = this->max_timesleep_ms_ - latency;
                    if(sleep_ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(int32_t(sleep_ms)));
                    } else {
                        // auto pop_latency = std::chrono::duration<double, std::milli>(end_pop - start).count();
                        // BOOST_LOG_TRIVIAL(warning) << "sleep_ms < 0 , pop_latency" << pop_latency << "latency " << latency
                        //     << "max_timesleep_ms_  " << this->max_timesleep_ms_;
                    }
                }
            });
        }

        this->collector_ = std::thread([this]() {
            T_Resp resp{};
            shared_ptr<Status> status{};
            void* obj;
            while(this->cli_->WaitResponse(resp, status, &obj)) {
                EachResp<T_Resp>* eachResp = (EachResp<T_Resp>*) obj;
                auto end = std::chrono::steady_clock::now();
                auto latency = std::chrono::duration<double, std::milli>(end - *(eachResp->start_t)).count();

                EachResp<T_Resp> respout{
                    eachResp->id,
                    eachResp->start_t,
                    resp,
                    eachResp->ref_resp,
                    status,
                    latency
                };
                this->pending_num_ --;
                if (this->pending_num_ == 0) {
                    this->cv_wait_.notify_all();
                }
                this->pending_compare_num_ ++;
                this->responses_.Push(respout);
            }
        });

        this->calculator_ = std::thread([this, do_compare]() {
            EachResp<T_Resp> resp{};
            while(this->responses_.WaitAndPop(resp) != QueueStatus::Closed) {
                bool ok{true};
                if (this->updateSummary(resp)) {
                    ok = do_compare(resp.response, resp.ref_resp, this->summary_data_.custom_summary);
                }
                this->pending_compare_num_ --;
                if (this->pending_compare_num_ == 0) {
                    this->summary_cv_wait_.notify_all();
                }
                if (!ok) {
                    break;
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
        this->summary_cv_wait_.notify_all();
        for (auto& thread : threads_) {
            thread.join();
        }
        this->calculator_.join();

        this->cli_->Close();
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
        this->pending_num_.store(0);
    }

    // wait all request
    bool WaitAllUntilTimeout(int64_t timeout_ms=0) {
        std::unique_lock<std::mutex> lock(this->mutex_);

        bool not_time_out{false};
        if (this->cv_wait_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] {
                return !this->enable_.load() || (this->enable_.load() && this->pending_num_.load() == 0); })) {
            not_time_out = true;
        }
        this->enable_.store(false);
        this->requests_.Close();
        this->pending_num_.store(0);
        return not_time_out;
    }

    SummaryType Summary() {
        std::unique_lock<std::mutex> lock(this->summary_mutex_);
        this->summary_cv_wait_.wait(lock, [this] {
            BOOST_LOG_TRIVIAL(warning) << "this->pending_num_ " << this->pending_num_ << " this->pending_compare_num_ " << this->pending_compare_num_;
            return this->pending_num_ <= 0 && this->pending_compare_num_ <= 0;
        });
        this->responses_.Close();

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
