IMAGE := alimama-max-code-2023

HAS_BUILDX = $(shell docker buildx > /dev/null 2>&1 && echo true || echo false)
BUILD = build
ifeq ($(HAS_BUILDX),true)
#  BUILD = buildx build --platform linux/amd64
endif

base:
	docker $(BUILD) -t $(IMAGE):base - < Dockerfile_base

init:
	@rm -rf code
	@mkdir -p code
	@cd code
	git clone --recurse-submodules -b v0.14.3 --depth 1 https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
	git clone --recurse-submodules -b v2.10.18 --depth 1 https://github.com/microsoft/cpprestsdk.git
	git clone --recurse-submodules -b v1.55.0 --depth 1 https://github.com/grpc/grpc


v0:
	@cp Dockerfile_v0 ./code/
	DOCKER_BUILDKIT=0 docker $(BUILD) -t $(IMAGE):v0 -f ./code/Dockerfile_v0 ./code
	@rm ./code/Dockerfile_v0
