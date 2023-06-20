#pragma once

#include <atomic>
#include "types.h"
#include "utils.h"

namespace alimama {

class IdGen {
 public:
  IdGen(serial_num_t start_serial_num = 0) : serial_num_(start_serial_num) {}

  inline id_t Gen() {
    id_t id = serial_num_.fetch_add(1);
    id = id << 32;
    id |= Random(0, UINT32_MAX);
    return id;
  }

 private:
  std::atomic<serial_num_t> serial_num_;
};

}  // namespace alimama