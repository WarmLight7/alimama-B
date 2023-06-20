#pragma once

#include <memory>
#include <string>

#include "index_factory.h"
#include "search.grpc.pb.h"
#include "service.h"
#include "thread_pool.h"
#include "utils.h"

namespace alimama {

class Proxy : public proto::SearchService::CallbackService,
              public ::alimama::Service {
 public:
  Proxy(const Enviroment& env) : ::alimama::Service(env) {}

  bool Start() override;

  ::grpc::ServerUnaryReactor* Search(
      ::grpc::CallbackServerContext* context,
      const ::alimama::proto::Request* request,
      ::alimama::proto::Response* response) override;

  auto& search_stubs() const { return search_stubs_; }

 private:
  std::vector<std::unique_ptr<proto::InnerSearchService::Stub>> search_stubs_;
};

}  // namespace alimama