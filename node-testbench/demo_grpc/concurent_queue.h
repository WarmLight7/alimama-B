#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

template<typename T>
class ConcurrentQueue {
public:
    ConcurrentQueue(int32_t capacity=0)
        : capacity_{capacity}, closed_{false} {
    }

    bool push(T const& v) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_push_.wait(lock, [this] {
            if (capacity_ == 0) {
                return closed_;
            } else {
                return q_.size() < capacity_ || closed_;
            }
        });
        if (closed_) return false;
        q_.push(v);
        lock.unlock();
        cv_pop_.notify_one();
        return true;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return q_.empty();
    }

    bool try_pop(T& popped_value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (q_.empty() || closed_) return false;
        popped_value = q_.front();
        q_.pop();
        cv_push_.notify_one();
        return true;
    }

    bool wait_and_pop(T& popped_value) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_pop_.wait(lock, [this] { return !q_.empty() || closed_; });
        if (closed_) return false;
        popped_value = q_.front();
        q_.pop();
        cv_push_.notify_one();
        return true;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        cv_push_.notify_all();
        cv_pop_.notify_all();
    }

private:
    std::queue<T> q_;
    mutable std::mutex mutex_;
    std::condition_variable cv_push_;
    std::condition_variable cv_pop_;
    int32_t capacity_;
    std::atomic<bool> closed_;
};
