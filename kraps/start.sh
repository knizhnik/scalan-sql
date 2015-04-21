n_nodes=16
nodes=""
for ((i=0;i<n_nodes;i++))
do
    nodes="$nodes localhost:600$i"
done
for ((i=0;i<n_nodes;i++))
do
    kraps/tpch $i $n_nodes $nodes > node$i.log 2>&1 &
done
wait
