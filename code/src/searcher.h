#pragma once

#include <memory>

#include "index_factory.h"
#include "search.grpc.pb.h"
#include "service.h"
#include "utils.h"

namespace alimama {

class Searcher : public proto::InnerSearchService::CallbackService,
                 public ::alimama::Service {
 public:
  Searcher(const Enviroment& env) : ::alimama::Service(env) {}

  bool Start() override;

  ::grpc::ServerUnaryReactor* Search(
      ::grpc::CallbackServerContext* context,
      const ::alimama::proto::Request* request,
      ::alimama::proto::InnerResponse* response) override;

  IndexFactory* index_factory() const { return index_factory_.get(); }

 private:
  std::unique_ptr<IndexFactory> index_factory_;
};

}  // namespace alimama