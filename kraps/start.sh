hosts=(lite01 lite02 lite03 lite04)
n_hosts=4
n_nodes=16
nodes=""
for ((i=0;i<n_nodes;i++))
do
    nodes="$nodes ${hosts[i % n_hosts]}:600$i"
done
for ((i=0;i<n_nodes;i++))
do
    ssh "${hosts[i % n_hosts]}" "export LD_LIBRARY_PATH=. ; kraps/tpch $i $n_nodes $nodes" > node$i.log 2>&1 &
done
wait
