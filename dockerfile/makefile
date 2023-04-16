build:
	docker build . -f Dockerfile -t alimama-2023 --progress=plain
test:
	docker container run --network host -it --rm alimama-2023
push:
	sudo docker tag alimama-2023 public-images-registry-vpc.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0
	sudo docker push public-images-registry-vpc.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0
push-public:
	sudo docker tag alimama-2023 public-images-registry.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0
	sudo docker push public-images-registry.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0
