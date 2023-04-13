set -ex

echo "0-Init"
cd `dirname $0`
rm node-1 node-2 node-3 build -rf || true

echo "1-Tar"
cd `dirname $0`
rm code.tar.gz -rf || true
tar -czf code.tar.gz --directory=code .

echo "2-Build"
cd `dirname $0`
mkdir build
tar -xzf code.tar.gz --director=build
docker run --rm  -v "./build:/work" -w /work --network none alimama-2023-engine bash build.sh

echo "3-Run"
cd `dirname $0`
cp build node-1 -rp
cp build node-2 -rp
cp build node-3 -rp
docker compose -f docker-compose-run.yml down
docker compose -f docker-compose-run.yml up -d

echo "4-Tail Log"
tail -F node-testbench/logs/test.log




