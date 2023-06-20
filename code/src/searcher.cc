#include "searcher.h"

#include "compare.h"
#include "etcd_client.h"

namespace alimama {

bool Searcher::Start() {
  ALI_LOG("Server: ", " starting");
  index_factory_ = std::make_unique<IndexFactory>();
  if (!env().data_file.empty()) {
    IndexFactory::Option option{env().column_id, env().column_num,
                                env().index_segment};
    if (!index_factory_->LoadIndexText(option, env().data_file)) {
      ALI_LOG("LoadIndexText from ", env().data_file, " fail.");
      return false;
    }
  } else {
    // TODO: binary index.
    ALI_LOG("Load binary index not implement.");
    return false;
  }

  auto etcd_client = std::make_unique<ETCDClient>();
  if (!etcd_client->Init()) {
    ALI_LOG("etcd client init fail. ");
    return false;
  }

  auto path = std::to_string(env().node_id);
  auto addr = GetHostIP() + ":" + std::to_string(env().server_port);

  if (!etcd_client->InnerRegister(path, addr)) {
    ALI_LOG("InnerRegister fail. path: ", path, " addr: ", addr);
    return false;
  }

  ALI_LOG("Server: ", path, " start success.");
  return true;
}

class SearcherSession {
 public:
  SearcherSession(Searcher* searcher, const ::alimama::proto::Request* request,
                  ::alimama::proto::InnerResponse* response,
                  ::grpc::ServerUnaryReactor* reactor)
      : searcher_(searcher),
        request_(request),
        response_(response),
        reactor_(reactor) {}

  void Run() {
    DoSearch();
    DoFinish();
  }

  void DoSearch() {
    // search
    std::vector<PayloadList*> docs;
    if (!searcher_->index_factory()->Search(request_->keywords(), &docs)) {
      return;
    }

    std::unordered_map<id_t, size_t> admaps;
    auto* ads = response_->mutable_ads();
    for (auto& pl : docs) {
      if (pl == nullptr) continue;
      for (auto& p : *pl) {
        // filter
        if ((p.timing_status & (1 << request_->hour())) == 0) {
          continue;
        }

        // score
        float ctr = (request_->context_vector(0) * p.vec[0] +
                     request_->context_vector(1) * p.vec[1]) +
                    0.000001f;
        float score = ctr * p.price;

        // uniq
        auto it = admaps.find(p.adgroup_id);
        if (it != admaps.end()) {
          auto* prev = ads->Mutable(it->second);
          if (prev->score() < score ||  // score high
              ((std::abs(prev->score() - score) < 0.000001f) &&
               (prev->ctr() < ctr))) {  // score equal && ctr high
            prev->set_ctr(ctr);
            prev->set_score(score);
          }
        } else {
          admaps[p.adgroup_id] = ads->size();
          auto ad = ads->Add();
          ad->set_adgroup_id(p.adgroup_id);
          ad->set_ctr(ctr);
          ad->set_score(score);
        }
      }
    }

    // topn
    auto topn = request_->topn();
    if (ads->size() > (int)request_->topn()) {
      topn = request_->topn() + 1;  // + 1 for last ad bidding.
    } else {
      topn = ads->size();
    }

    auto comper = [](const proto::Ad& l, const proto::Ad& r) {
      if (l.score() > r.score()) return true;
      if (l.score() < r.score()) return false;
      if (l.ctr() > r.ctr()) return true;
      return false;
    };

    float thres = ads->size() * 1.0f / topn;
    if (std::pow(thres, thres) > topn) {
      std::partial_sort(ads->begin(), ads->begin() + topn, ads->end(), comper);
      std::sort(ads->begin(), ads->begin() + topn, comper);
    } else {
      std::sort(ads->begin(), ads->end(), comper);
    }

    for (; ads->size() > (int)topn;) {
      ads->RemoveLast();
    }
  }

  void DoFinish() {
    reactor_->Finish(::grpc::Status::OK);
    delete this;
  }

 private:
  Searcher* searcher_;
  const ::alimama::proto::Request* request_;
  ::alimama::proto::InnerResponse* response_;
  ::grpc::ServerUnaryReactor* reactor_;

  std::unordered_map<id_t, size_t> admaps;
};

::grpc::ServerUnaryReactor* Searcher::Search(
    ::grpc::CallbackServerContext* context,
    const ::alimama::proto::Request* request,
    ::alimama::proto::InnerResponse* response) {
  auto reactor = context->DefaultReactor();
  auto session = new SearcherSession(this, request, response, reactor);
  if (!thread_pool()->Schedule([session]() { session->Run(); })) {
    // schedule fail. discord this rpc.
    delete session;
    reactor->Finish(::grpc::Status::OK);
  }
  return reactor;
}

}  // namespace alimama