CXX = g++

#OPT_FLAGS = -O0 -g
OPT_FLAGS = -O2 -g

CPPFLAGS += `pkg-config --cflags protobuf grpc` 
CXXFLAGS += -Wall -std=c++17 $(OPT_FLAGS) 

LDFLAGSBASE += -pthread\
           -ldl
LDFLAGS += -L/usr/local/lib -letcd-cpp-api -lcpprest `pkg-config --libs --static protobuf grpc++`\
           $(LDFLAGSBASE)

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTO_PATH = ./

vpath %.proto $(PROTO_PATH)

.PHONY: default
default: server

.PHONY: all
all: data-maker server client

data-maker: data_maker.o
	$(CXX) $^ $(LDFLAGSBASE) $(OPT_FLAGS) -o $@

server: search.pb.o search.grpc.pb.o proxy.o searcher.o index_factory.o etcd_client.o thread_pool.o server.o
	$(CXX) $^ $(LDFLAGS) $(OPT_FLAGS) -o $@

client: search.pb.o search.grpc.pb.o etcd_client.o client.o
	$(CXX) $^ $(LDFLAGS) $(OPT_FLAGS) -o $@

.PHONY: test
test: index-factory-test etcd-client-test

index-factory-test: index_factory.o index_factory_test.o
	$(CXX) $^ $(LDFLAGS) $(OPT_FLAGS) -o $@

etcd-client-test: etcd_client.o etcd_client_test.o
	$(CXX) $^ $(LDFLAGS) $(OPT_FLAGS) -o $@

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ -c $<

.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTO_PATH) --grpc_out=./ --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTO_PATH) --cpp_out=./ $<

clean:
	rm -f *.o *.pb.h *.pb.cc server client index-factory-test etcd-client-test