n_nodes=64
nodes=""
for ((i=0;i<n_nodes;i++))
do
    nodes="$nodes localhost:700$i"
done
for ((i=0;i<n_nodes;i++))
do
    ./tpch -cache -dir /srv/remote/all-common/tpch/data -split 4 $i $n_nodes $nodes > node$i.log 2>&1 &
done
wait
