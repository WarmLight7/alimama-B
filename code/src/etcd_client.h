#pragma once

#include <etcd/Response.hpp>
#include <etcd/SyncClient.hpp>
#include <etcd/Watcher.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace alimama {
class ETCDClient {
 public:
  ETCDClient(const std::string& endpoint = "http://etcd:2379",
             const std::string public_path = "/services/searchservice",
             const std::string inner_path = "/services/innerservices");

  bool Init();

  bool Publish(const std::string& addr);
  std::string GetPublish();

  bool InnerRegister(const std::string& nodeid, const std::string& addr);
  std::unordered_map<std::string, std::string> GetInnerRegister();

 private:
  std::string endpoint_;
  std::string public_path_;
  std::string inner_path_;

  std::unique_ptr<etcd::SyncClient> etcd_client_;
};
}  // namespace alimama