hosts=(k9-01 k9-02 k9-03 k9-04 k9-05 k9-06 k9-07 k9-08)
n_hosts=$1
n_nodes=$2
split=$((n_nodes / 16))
nodes=""
for ((i=0;i<n_nodes;i++))
do
    nodes="$nodes ${hosts[i % n_hosts]}:$((6000 + i))"
done
rm -f node*.log
for ((i=0;i<n_nodes;i++))
do
	ssh ${hosts[i % n_hosts]} "./tpch -cache -dir /srv/remote/all-common/tpch/data -split $split $i $n_nodes $nodes" > node$i.log 2>&1 &
done
wait
