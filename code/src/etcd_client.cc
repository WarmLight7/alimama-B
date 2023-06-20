#include "etcd_client.h"

#include "utils.h"

namespace alimama {

ETCDClient::ETCDClient(const std::string& endpoint,
                       const std::string public_path,
                       const std::string inner_path)
    : endpoint_(endpoint), public_path_(public_path), inner_path_(inner_path) {}

bool ETCDClient::Init() {
  etcd_client_.reset(new etcd::SyncClient(endpoint_));
  return true;
}

bool ETCDClient::Publish(const std::string& addr) {
  auto resp = etcd_client_->set(public_path_, addr);
  if (!resp.is_ok()) {
    ALI_LOG("publish server fail. ", resp.error_message());
    return false;
  }
  return true;
}

std::string ETCDClient::GetPublish() {
  auto resp = etcd_client_->get(public_path_);
  if (!resp.is_ok()) {
    ALI_LOG("get key fail. key: ", public_path_, ", err: ", resp.error_message());
    return "";
  }
  return resp.value().as_string();
}

bool ETCDClient::InnerRegister(const std::string& nodeid,
                               const std::string& addr) {
  auto resp = etcd_client_->set(inner_path_ + '/' + nodeid, addr);
  if (!resp.is_ok()) {
    ALI_LOG("inner Resgister fail. ", resp.error_message());
    return false;
  }
  return true;
}

std::unordered_map<std::string, std::string> ETCDClient::GetInnerRegister() {
  std::unordered_map<std::string, std::string> ret;
  auto resp = etcd_client_->ls(inner_path_);
  if (!resp.is_ok()) {
    ALI_LOG("get key fail. key: ", inner_path_, ", err: ", resp.error_message());
    return ret;
  }
  for (size_t i = 0; i < resp.values().size(); ++i) {
    auto n = resp.key(i).substr(inner_path_.size() + 1);
    auto addr = resp.value(i).as_string();
    ret[n] = addr;
  }
  return ret;
}

}  // namespace alimama