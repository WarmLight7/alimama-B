

mkdir -p logs/
while true
do
	echo node_1 `etcdctl --endpoints http://etcd:2379 get node_1`  >> logs/test.log
	echo node_2 `etcdctl --endpoints http://etcd:2379 get node_2`  >> logs/test.log
	echo node_3 `etcdctl --endpoints http://etcd:2379 get node_3`  >> logs/test.log
	sleep 1
done
