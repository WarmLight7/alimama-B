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
struct Summary {
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
    ConcurrentQueue<EachReqInternal<T_Req>> requests;
    ConcurrentQueue<EachResp<T_Resp>> responses;
    std::vector<std::thread> threads;
    std::atomic<bool> enable;
    std::atomic<int32_t> pending_num;
    std::mutex mutex_;
    std::condition_variable cv_;

    int64_t deadline_ms;
public:
    using DoRequestFunc = std::function<Status(ClientContext&, T_Req&, T_Resp&)>;
    using SummaryType = Summary<T_Resp>;
    GrpcBenchmark(DoRequestFunc do_request, int32_t threads_num=1, int64_t deadline_ms=10, int32_t qps_limit=1000):
        threads(threads_num), enable(true), pending_num{0}, deadline_ms{deadline_ms} {
        auto max_timesleep_ms = 1 * 1000/(qps_limit * 1.0 / threads_num);
        std::cout << " threads_num " << threads_num << " deadline_ms " << deadline_ms << " qps_limit " << qps_limit << std::endl;

        for (size_t i = 0; i < this->threads.size(); ++i) {
            threads[i] = std::thread([&]() {
                while (this->enable.load()) {
                    EachReqInternal<T_Req> each;
                    while (!this->requests.try_pop(each) && this->enable) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                    ClientContext context;
                    auto start = std::chrono::steady_clock::now();
                    gpr_timespec timeout = gpr_time_from_millis(this->deadline_ms, GPR_TIMESPAN);
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
                    this->responses.push(eachResp);
                    auto sleep_ms = max_timesleep_ms - latency;
                    if(sleep_ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(int32_t(sleep_ms)));
                    }
                    this->pending_num --;
                }
            });
        }
    }
    GrpcBenchmark(const GrpcBenchmark&) = delete;
    GrpcBenchmark(GrpcBenchmark&&) = delete;
    GrpcBenchmark& operator=(const GrpcBenchmark&) = delete;
    GrpcBenchmark& operator=(GrpcBenchmark&&) = delete;

    ~GrpcBenchmark() {
        this->enable.store(false);
        for (size_t i = 0; i < this->threads.size(); ++i) {
            this->threads[i].join();
        }
    }

    void request(uint64_t id, const T_Req& req) {
        this->pending_num ++;
        EachReqInternal<T_Req> internal {
            id,
            req
        };
        this->requests.push(internal);
    }

    void wait_all() {
        while (this->pending_num > 0 && this->enable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // return false if timeout
    bool wait_all_until_timeout(int64_t timeout_ms=0) {
        auto start = std::chrono::steady_clock::now();
        while (this->pending_num > 0 && this->enable) {
            auto current = std::chrono::steady_clock::now();
            auto elapsedtime_ms = std::chrono::duration<double, std::milli>(current - start).count();
            if (timeout_ms > 0 && elapsedtime_ms > timeout_ms) {
                this->enable = false;
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    Summary<T_Resp> summary() {
        Summary<T_Resp> resps{};
        EachResp<T_Resp> resp;
        double total_latency = 0;
        boost::heap::priority_queue<double> pq{}; 
        while(this->responses.try_pop(resp)) {
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
