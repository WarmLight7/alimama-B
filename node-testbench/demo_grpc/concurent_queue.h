#pragma once
#include <queue>
#include <boost/thread.hpp>

template<typename T>
class ConcurrentQueue {
public:
    void push(T const& v) {
        boost::mutex::scoped_lock lock(mutex_);
        q_.push(v);
        lock.unlock();
        cv_.notify_one();
    }

    bool empty() const {
        boost::mutex::scoped_lock lock(mutex_);
        return q_.empty();
    }

    bool try_pop(T& popped_value) {
        boost::mutex::scoped_lock lock(mutex_);
        if (q_.empty())
            return false;
        popped_value = q_.front();
        q_.pop();
        return true;
    }

    void wait_and_pop(T& popped_value) {
        boost::mutex::scoped_lock lock(mutex_);
        while (q_.empty())
            cv_.wait(lock);
        popped_value = q_.front();
        q_.pop();
    }
private:
    std::queue<T> q_;
    mutable boost::mutex mutex_;
    boost::condition_variable cv_;
};