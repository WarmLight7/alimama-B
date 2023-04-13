
while true 
do
	echo `date` ${NODE_ID} >> logs/run.log
	etcdctl --endpoints http://etcd:2379 put node_${NODE_ID} `date +%Y-%m-%d_%H-%M-%S`
	sleep 3
done


