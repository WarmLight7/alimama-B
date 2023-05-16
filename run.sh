
#这个只是当前的 demo 模板，后续需要根据需求拆分，比如拆分成 build.sh, tar-submit.sh，debug.sh这样的
set -ex

echo "0-Init"
cd `dirname $0`
rm node-1 node-2 node-3 build -rf || true

echo "1-Tar"
cd `dirname $0`
rm code.tar.gz -rf || true
#考生相关：指定这个code.tar.gz就是用于提交的代码压缩包
tar -czf code.tar.gz --directory=code .

echo "2-Build"
cd `dirname $0`
mkdir build
tar -xzf code.tar.gz --director=build
#考生相关: build.sh是构建入口脚本, run.sh是运行入口脚本
docker run --rm  -v "./build:/work" -w /work --network none public-images-registry.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0 bash build.sh
docker run --rm  -v "./build:/work" -w /work --network none public-images-registry.cn-hangzhou.cr.aliyuncs.com/demo/alimama-2023:v0 bash build.sh

echo "3-Run"
cd `dirname $0`
cp build node-1 -rp
cp build node-2 -rp
cp build node-3 -rp
docker compose -f docker-compose-run.yml down
docker compose -f docker-compose-run.yml up -d

inotifywait -e modify "node-testbench/testbenchResult.json"