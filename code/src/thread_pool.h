#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace alimama {

class ThreadPool {
 public:
  ThreadPool(size_t thread_num, size_t queue_size);
  ~ThreadPool();

  using Task = std::function<void()>;
  bool Schedule(Task task);

  void Shutdown();

 private:
  void Loop();

 private:
  class TaskQueue;
  TaskQueue* queue_;
  std::vector<std::thread> threads_;
  std::atomic<bool> stop_;
};
}  // namespace alimama