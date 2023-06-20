#include "etcd_client.h"

#include <chrono>
#include <thread>

#include "utils.h"

using namespace std::chrono_literals;

int main(int argc, char* argv[]) {
  auto etcd_client = new alimama::ETCDClient();
  if (!etcd_client->Init()) {
    ALI_LOG("ETCDClient Init fail.");
    return -1;
  }

  if (!etcd_client->InnerRegister("2", "node1:11111")) {
    ALI_LOG("ETCDClient InnerRegister 2 fail.");
    return -1;
  }
  if (!etcd_client->InnerRegister("3", "node1:22222")) {
    ALI_LOG("ETCDClient InnerRegister 3 fail.");
    return -1;
  }

  while (true) {
    auto inner_endpoints = etcd_client->GetInnerRegister();
    for (auto& p : inner_endpoints) {
      std::cout << "name: " << p.first << ", addr: " << p.second << std::endl;
    }

    if (inner_endpoints.size() == 2) {
      break;
    }
    std::this_thread::sleep_for(1s);
  }

  delete etcd_client;
  while (true) std::this_thread::sleep_for(1s);
  return 0;
}