#include "proxy.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>

#include "compare.h"
#include "etcd_client.h"
#include "utils.h"

using namespace std::chrono_literals;

namespace alimama {

bool Proxy::Start() {
  auto etcd_client = std::make_unique<ETCDClient>();
  if (!etcd_client->Init()) {
    ALI_LOG("etcd client init fail. ");
    return false;
  }

  if (env().node_num <= 1) {
    ALI_LOG("node number must > 1, got: ", env().node_num);
    return false;
  }

  size_t searcher_num = env_.node_num - 1;

  auto inner_endpoints = etcd_client->GetInnerRegister();
  for (int i = 0; i < 600 && inner_endpoints.size() != searcher_num; ++i) {
    std::this_thread::sleep_for(1s);
    inner_endpoints = etcd_client->GetInnerRegister();
  }

  if (inner_endpoints.size() != searcher_num) {
    ALI_LOG("wait server start fail.");
    return false;
  }

  for (auto& e : inner_endpoints) {
    search_stubs_.emplace_back(proto::InnerSearchService::NewStub(
        ::grpc::CreateChannel(e.second, ::grpc::InsecureChannelCredentials())));
  }
  auto addr = GetHostIP() + ":" + std::to_string(env().server_port);
  if (!etcd_client->Publish(addr)) {
    ALI_LOG("publish proxy fail. ", addr);
    return false;
  }

  ALI_LOG("Proxy start success.");
  return true;
}

class ProxySession {
 public:
  explicit ProxySession(Proxy* p, const ::alimama::proto::Request* req,
                        ::alimama::proto::Response* resp,
                        ::grpc::ServerUnaryReactor* reactor, int c)
      : proxy_(p),
        request_(req),
        response_(resp),
        reactor_(reactor),
        counts_(c),
        status_(ProxySession::Status::Init) {
    for (int i = 0; i < c; ++i) {
      inner_context_.emplace_back(std::make_unique<::grpc::ClientContext>());
    }
    inner_response_.resize(c);
    inner_status_.resize(c);
  }

  void SearchCallback(::grpc::Status status, size_t i) {
    inner_status_[i] = status;
    if (--counts_ > 0) return;
    proxy_->thread_pool()->Schedule([this]() { Run(); });
  }

  void DoSearch() {
    const auto& stubs = proxy_->search_stubs();
    for (size_t i = 0; i < stubs.size(); ++i) {
      auto deadline = std::chrono::system_clock::now() +
                      std::chrono::milliseconds(proxy_->env().timeout_ms);
      inner_context_[i]->set_deadline(deadline);
      auto callback = [this, i](::grpc::Status status) {
        SearchCallback(std::move(status), i);
      };
      stubs[i]->async()->Search(inner_context_[i].get(), request_,
                                &inner_response_[i], callback);
    }
  }

  void DoMerge() {
    for (size_t i = 0; i < inner_status_.size(); ++i) {
      // if one fails, discard this request.
      if (!inner_status_[i].ok()) {
        return;
      }
    }

    size_t topn = 0;
    for (auto& resp : inner_response_) topn += resp.ads_size();
    if (topn > request_->topn()) topn = request_->topn() + 1;

    ads_.reserve(topn);

    if (inner_response_.size() == 1) {
      auto& resp = inner_response_[0];
      for (int i = 0; i < resp.ads_size() && ads_.size() < topn; ++i) {
        ads_.push_back(resp.mutable_ads(i));
      }
    } else if (inner_status_.size() == 2) {
      BinaryMerge(topn);
    } else {
      // TODO:
    }
  }

  void DoBid() {
    size_t i = 0;
    for (; i + 1 < ads_.size(); ++i) {
      uint64_t price = ads_[i + 1]->score() / ads_[i]->ctr() + 0.5f;
      response_->add_adgroup_ids(ads_[i]->adgroup_id());
      response_->add_prices(price);
    }
    // last ad use self price.
    if (ads_.size() > 0) {
      response_->add_adgroup_ids(ads_[i]->adgroup_id());
      response_->add_prices(ads_[i]->score() / ads_[i]->ctr() + 0.5f);
    }

    if (response_->adgroup_ids_size() > (int)request_->topn()) {
      response_->mutable_adgroup_ids()->Truncate(request_->topn());
      response_->mutable_prices()->Truncate(request_->topn());
    }
  }

  void DoFinish() {
    reactor_->Finish(::grpc::Status::OK);
    delete this;
  }

  void Run() {
    switch (status_) {
      case Status::Init: {
        status_ = Status::Search;
        DoSearch();
        return;
      }
      case Status::Search: {
        DoMerge();
        DoBid();
        DoFinish();
        return;
      }
    }
  }

 private:
  void BinaryMerge(size_t topn) {
    auto& resp_i = inner_response_[0];
    auto& resp_j = inner_response_[1];
    int i = 0;
    int j = 0;
    for (; i < resp_i.ads_size() && j < resp_j.ads_size() &&
           ads_.size() < topn;) {
      if (AdComp(resp_i.ads(i), resp_j.ads(j))) {
        ads_.push_back(resp_i.mutable_ads(i));
        i++;
      } else {
        ads_.push_back(resp_j.mutable_ads(j));
        j++;
      }
    }

    for (; i < resp_i.ads_size() && ads_.size() < topn; ++i) {
      ads_.push_back(resp_i.mutable_ads(i));
    }

    for (; j < resp_j.ads_size() && ads_.size() < topn; ++j) {
      ads_.push_back(resp_j.mutable_ads(j));
    }
  }

 private:
  Proxy* proxy_;
  const ::alimama::proto::Request* request_;
  ::alimama::proto::Response* response_;
  ::grpc::ServerUnaryReactor* reactor_;

  std::vector<std::unique_ptr<::grpc::ClientContext>> inner_context_;
  std::vector<::alimama::proto::InnerResponse> inner_response_;
  std::vector<::alimama::proto::Ad*> ads_;
  std::vector<::grpc::Status> inner_status_;

  std::atomic<int> counts_;

  enum class Status { Init, Search } status_;
};

::grpc::ServerUnaryReactor* Proxy::Search(
    ::grpc::CallbackServerContext* context,
    const ::alimama::proto::Request* request,
    ::alimama::proto::Response* response) {
  auto reactor = context->DefaultReactor();
  auto session =
      new ProxySession(this, request, response, reactor, search_stubs_.size());

  if (!thread_pool()->Schedule([session]() { session->Run(); })) {
    // schedule fail. discord this rpc.
    delete session;
    reactor->Finish(::grpc::Status::OK);
  }
  return reactor;
}

}  // namespace alimama