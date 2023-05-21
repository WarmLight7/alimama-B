#!/bin/bash
docker run --rm  -v "./testbench:/work" -w /work --network none public-images-registry.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0 bash build.sh

docker compose stop testbench
docker compose up -d
docker compose logs -f testbench