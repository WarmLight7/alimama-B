#include <grpcpp/grpcpp.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "etcd_client.h"
#include "search.grpc.pb.h"
#include "utils.h"

using alimama::proto::Request;
using alimama::proto::Response;
using alimama::proto::SearchService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class SearchClient {
 public:
  SearchClient(int mode, const std::string server_addr,
               const std::string& req_file, const std::string& res_file)
      : mode_(mode),
        stub_(SearchService::NewStub(grpc::CreateChannel(
            server_addr, grpc::InsecureChannelCredentials()))),
        req_file_(req_file),
        res_file_(res_file) {}

  bool ParseLineToRequest(const std::string& line, Request* request) {
    std::vector<std::string_view> sections;
    alimama::SplitStr(line, " ", &sections);
    if (sections.size() != 4) {
      return false;
    }

    std::vector<std::string_view> keywords;
    alimama::SplitStr(sections[0], ",", &keywords);
    char* dummy = nullptr;
    for (auto& ks : keywords) {
      request->add_keywords(strtoull(ks.data(), &dummy, 10));
    }
    request->set_hour(strtoull(sections[2].data(), &dummy, 10));
    request->set_topn(strtoull(sections[3].data(), &dummy, 10));
    std::vector<std::string_view> vecs;
    alimama::SplitStr(sections[1], ",", &vecs);
    for (auto& vs : vecs) {
      request->add_context_vector(strtof(vs.data(), &dummy));
    }
    return true;
  }

  template <typename In, typename Out>
  void DoSearch(In* in, Out* out, bool human_read = false) {
    std::string line;
    Request request;
    Response response;
    while (std::getline(*in, line)) {
      if (line.empty() || line == "Q" || line == "q") {
        break;
      }

      request.Clear();
      response.Clear();
      if (!ParseLineToRequest(line, &request)) {
        (*out) << "line parse fail.\n";
        continue;
      }

      if (human_read) {
        (*out) << "Client send: \n" << request.DebugString() << "\n";
      }

      ClientContext context;
      alimama::TimeKeeper tk;
      Status status = stub_->Search(&context, request, &response);
      if (human_read) {
        (*out) << "cost: " << tk.Escape() << "ms\n";
      }
      if (status.ok()) {
        if (human_read) {
          (*out) << "Client received: \n" << response.DebugString() << "\n";
        } else {
          (*out) << response.ShortDebugString() << "\n";
        }
      } else {
        (*out) << "RPC ERROR: " << status.error_message() << "\n";
      }
    }
  }

  void Search() {
    if (mode_ == 0) {
      DoSearch(&std::cin, &std::cout, true);
      return;
    }

    std::ifstream ifs{req_file_};
    std::ofstream ofs{res_file_, std::ios::trunc};
    DoSearch(&ifs, &ofs);
  }

 private:
  int mode_;
  std::unique_ptr<SearchService::Stub> stub_;
  std::string req_file_;
  std::string res_file_;
};

std::string GetAddrFromETCD() {
  alimama::ETCDClient etcd_client;
  if (!etcd_client.Init()) {
    ALI_LOG("etcd_client Init fail.");
    return "";
  }

  std::string addr = etcd_client.GetPublish();
  if (addr.empty()) {
    ALI_LOG("not found publish service addr.");
    return "";
  }
  return addr;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Usage: \n";
    std::cout << "\tclient [options]\n\n";
    std::cout << "Options: \n";
    std::cout << "\t--mode 0: requests from std::in, 1: requests from file.\n";
    std::cout << "\t--req_file requests file.\n";
    std::cout << "\t--res_file responses file.\n";
    std::cout << "\t--server_addr server address.\n";
    std::cout << "\t--server_etcd_path server etcd path.\n";
    return 0;
  }
  int mode = 0;
  std::string req_file;
  std::string res_file;
  std::string server_addr;

  for (int i = 1; i < argc - 1; i += 2) {
    if (STR_EQ(argv[i], "--mode")) {
      mode = atoi(argv[i + 1]);
    } else if (STR_EQ(argv[i], "--req_file")) {
      req_file = argv[i + 1];
    } else if (STR_EQ(argv[i], "--res_file")) {
      res_file = argv[i + 1];
    } else if (STR_EQ(argv[i], "--server_addr")) {
      server_addr = argv[i + 1];
    } else {
      ALI_LOG("unkown options: ", argv[i]);
      return -1;
    }
  }

  if (mode == 1 && (req_file.empty() || res_file.empty())) {
    ALI_LOG("mode 1: need set file by --req_file and --res_file");
    return -1;
  }

  if (server_addr.empty()) {
    server_addr = GetAddrFromETCD();
  }

  if (server_addr.empty()) {
    ALI_LOG("server addr nedd set by --server_addr or --server_etcd_path");
    return -1;
  }

  ALI_LOG("dail addr: ", server_addr);
  SearchClient client(mode, server_addr, req_file, res_file);

  client.Search();
  return 0;
}