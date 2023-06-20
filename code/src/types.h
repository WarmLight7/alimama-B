#pragma once

#include <climits>
#include <cstdint>

namespace alimama {

typedef uint32_t serial_num_t;
constexpr serial_num_t SERIAL_NUM_MAX = UINT32_MAX;

typedef uint64_t id_t;
constexpr id_t ID_MAX = ULLONG_MAX;

inline constexpr serial_num_t get_serial_num(id_t id) {
  return (id & 0xFFFFFFFF00000000) >> 32;
}

typedef uint32_t column_id_t;
typedef uint32_t segment_id_t;

}  // namespace alimama