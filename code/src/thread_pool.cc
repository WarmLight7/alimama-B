#include "thread_pool.h"

#include <condition_variable>

namespace alimama {

class ThreadPool::TaskQueue {
 public:
  explicit TaskQueue(size_t capacity) {
    capacity_ = capacity;
    data_ = new ThreadPool::Task[capacity];
    size_ = 0;
    front_ = 0;
    back_ = 0;
  }

  ~TaskQueue() { delete[] data_; }

  bool Push(ThreadPool::Task task) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (size_ >= capacity_) {
      return false;
    }

    data_[back_++] = std::move(task);
    if (back_ == capacity_) back_ = 0;
    ++size_;
    lock.unlock();
    cv_.notify_one();
    return true;
  }

  ThreadPool::Task Pop() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (size_ == 0) {
      cv_.wait(lock);
    }
    auto task = std::move(data_[front_++]);
    if (front_ == capacity_) front_ = 0;
    --size_;
    return task;
  }

 private:
  size_t capacity_;
  size_t size_;
  size_t front_;
  size_t back_;
  ThreadPool::Task* data_;

  std::mutex mtx_;
  std::condition_variable cv_;
};

ThreadPool::ThreadPool(size_t thread_num, size_t queue_size) : stop_(false) {
  queue_ = new TaskQueue(queue_size);
  for (size_t i = 0; i < thread_num; ++i) {
    threads_.emplace_back([this]() { Loop(); });
  }
}

ThreadPool::~ThreadPool() {
  for (auto& th : threads_) {
    th.join();
  }
  delete queue_;
}

void ThreadPool::Shutdown() { stop_ = true; }

bool ThreadPool::Schedule(ThreadPool::Task task) {
  return queue_->Push(std::move(task));
}

void ThreadPool::Loop() {
  while (!stop_) {
    auto task = queue_->Pop();
    task();
  }
}

}  // namespace alimama