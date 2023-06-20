#pragma once

#include <ifaddrs.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "types.h"

template <typename... Args>
void Log(Args&&... args) {
  (std::cout << ... << args);
  std::cout << std::endl;
}

#define ALI_LOG(...) \
  Log(__FILE__, ":", __LINE__, " [", __FUNCTION__, "]: ", __VA_ARGS__)

namespace alimama {

// random values uniformly distributed on the interval [a, b-1]
inline uint64_t Random(uint64_t a, uint64_t b) {
  static thread_local std::random_device rd;
  static thread_local std::mt19937_64 gen(rd());
  if (a == b) return a;
  if (a > b) std::swap(a, b);
  return gen() % (b - a) + a;
}

#define STR_EQ(a, b) (strncmp(a, b, sizeof(b)) == 0)

template <typename T>
inline void SplitStr(const T& str, const std::string_view& sep,
                     std::vector<std::string_view>* segments) {
  segments->clear();
  size_t b = 0;
  size_t e = 0;

  while ((e = str.find(sep, e)) != T::npos) {
    if (b != e) {
      segments->emplace_back(str.data() + b, e - b);
    }
    e += sep.size();
    b = e;
  }

  if (b < str.size()) {
    segments->emplace_back(str.data() + b, str.size() - b);
  }
}

struct TimeKeeper {
  TimeKeeper() { ::gettimeofday(&s, NULL); }

  void Reset() { ::gettimeofday(&s, NULL); }

  float Escape() {
    timeval e;
    ::gettimeofday(&e, NULL);
    return (e.tv_sec - s.tv_sec) * 1000.0f + (e.tv_usec - s.tv_usec) / 1000.0f;
  }

  timeval s;
};

template <typename T>
inline bool GetEnv(const std::string& key, T* val) {
  auto v = std::getenv(key.c_str());
  if (!v) return false;
  *val = std::atoi(v);
  return (*val != 0) || (v[0] == '0');
}

template <>
inline bool GetEnv(const std::string& key, std::string* val) {
  auto v = std::getenv(key.c_str());
  if (!v) return false;
  val->assign(v);
  return true;
}

inline std::string GetHostIP() {
  struct ifaddrs* ifaddr;
  char host[NI_MAXHOST];

  if (getifaddrs(&ifaddr) == -1) {
    return "";
  }

  for (auto ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    int family = ifa->ifa_addr->sa_family;

    if (family == AF_INET) {
      if (strncmp(ifa->ifa_name, "lo", 2) == 0) continue;

      if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host,
                      NI_MAXHOST, NULL, 0, NI_NUMERICHOST) == 0) {
        freeifaddrs(ifaddr);
        return std::string(host);
      }
    }
  }

  freeifaddrs(ifaddr);
  return "";
}

}  // namespace alimama
