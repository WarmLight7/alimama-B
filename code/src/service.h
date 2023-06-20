#pragma once

#include <memory>
#include <string>

#include "thread_pool.h"
#include "types.h"

namespace alimama {

struct Enviroment {
  int32_t node_id;
  int32_t node_num;
  enum class Role { Proxy, Searcher } role;

  uint16_t server_port = 9527;
  uint32_t timeout_ms = 100;
  uint32_t worker_num = 16;
  uint32_t queue_size = 1024;

  uint32_t index_segment = 16;

  std::string data_file;
  std::string index_path;

  column_id_t column_id = 0;
  column_id_t column_num = 1;
};

class Service {
 public:
  explicit Service(const Enviroment& env)
      : env_(env),
        thread_pool_(new ThreadPool(env.worker_num, env.queue_size)) {}
  virtual ~Service() = default;

  virtual bool Start() = 0;

  const Enviroment& env() const { return env_; }
  ThreadPool* thread_pool() const { return thread_pool_.get(); }

 protected:
  Enviroment env_;
  std::unique_ptr<ThreadPool> thread_pool_;
};

}  // namespace alimama