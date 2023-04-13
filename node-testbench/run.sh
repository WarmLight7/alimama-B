

mkdir -p logs/run.log
for i in {1..100}
do
	echo node_1 `etcdctl --endpoints http://etcd:2379 get node_1`  >> logs/run.log
	echo node_2 `etcdctl --endpoints http://etcd:2379 get node_2`  >> logs/run.log
	echo node_3 `etcdctl --endpoints http://etcd:2379 get node_3`  >> logs/run.log
	sleep 1
done
