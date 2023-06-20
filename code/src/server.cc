#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>

#include "proxy.h"
#include "searcher.h"
#include "service.h"
#include "utils.h"

bool InitEnviroment(int argc, char** argv, alimama::Enviroment* env) {
  for (int i = 1; i < argc - 1; i += 2) {
    if (STR_EQ(argv[i], "--timeout")) {
      env->timeout_ms = atoi(argv[i + 1]);
    } else if (STR_EQ(argv[i], "--worker_num")) {
      env->worker_num = atoi(argv[i + 1]);
    } else if (STR_EQ(argv[i], "--data_file")) {
      env->data_file = argv[i + 1];
    } else if (STR_EQ(argv[i], "--index_path")) {
      env->index_path = argv[i + 1];
    } else if (STR_EQ(argv[i], "--server_port")) {
      env->server_port = atol(argv[i + 1]);
    } else {
      ALI_LOG("unkown options: ", argv[i]);
      return false;
    }
  }

  if (!env->data_file.empty() && !env->index_path.empty()) {
    ALI_LOG("can't set data_file and index_path meanwhile.");
    return false;
  }

  if (!alimama::GetEnv("NODE_ID", &env->node_id)) {
    ALI_LOG("get NODE_ID from env fail.");
    return false;
  }

  if (!alimama::GetEnv("NODE_NUM", &env->node_num)) {
    ALI_LOG("get NODE_NUM from env fail.");
    return false;
  }

  env->column_id = env->node_id - 2;
  env->column_num = env->node_num - 1;
  env->server_port = env->server_port + (env->node_id - 1);

  env->role = (env->node_id == 1) ? alimama::Enviroment::Role::Proxy
                                  : alimama::Enviroment::Role::Searcher;

  return true;
}

bool MakeService(const alimama::Enviroment& env,
                 std::unique_ptr<::grpc::Service>* grpc_service,
                 alimama::Service** mama_service) {
  switch (env.role) {
    case alimama::Enviroment::Role::Proxy: {
      auto proxy = new alimama::Proxy(env);
      grpc_service->reset(proxy);
      *mama_service = proxy;
      return true;
    }

    case alimama::Enviroment::Role::Searcher: {
      auto searcher = new alimama::Searcher(env);
      grpc_service->reset(searcher);
      *mama_service = searcher;
      return true;
    }

    default:
      ALI_LOG("unknown role: ", static_cast<int>(env.role));
      return false;
  }
  return false;
}

int main(int argc, char** argv) {
  ALI_LOG("Server: ", " starting 0");
  alimama::Enviroment env;
  if (!InitEnviroment(argc, argv, &env)) {
    ALI_LOG("InitEnviroment fail.");
    return -1;
  }

  std::string server_address = "0.0.0.0:" + std::to_string(env.server_port);
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  std::unique_ptr<::grpc::Service> grpc_service;
  alimama::Service* mama_service = nullptr;

  if (!MakeService(env, &grpc_service, &mama_service)) {
    ALI_LOG("MakeService fail.");
    return -1;
  }

  builder.RegisterService(grpc_service.get());
  std::unique_ptr<::grpc::Server> server(builder.BuildAndStart());
  if (!mama_service->Start()) {
    ALI_LOG("service Start fail.");
    return -1;
  } else {
    server->Wait();
  }
  return 0;
}