n_nodes=4
nodes=""
pkill -9 tpch
for ((i=0;i<n_nodes;i++))
do
    nodes="$nodes localhost:700$i"
done
for ((i=0;i<n_nodes;i++))
do
    ./tpch -dir /home/knizhnik/tpch-dbgen $i $n_nodes $nodes > node$i.log 2>&1 &
done
wait
