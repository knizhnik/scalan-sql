hosts=(lite01 lite02 lite03 lite04)
n_hosts=4
n_nodes=16
nodes=""
for ((i=0;i<n_nodes;i++))
do
    nodes="$nodes ${hosts[i % n_hosts]}:500$i"
done
hadoop_home=/home/mpiuser/hadoop_home/share/hadoop
hdfs_classpath="$hadoop_home/hdfs/*.jar $hadoop_home/hdfs/lib/*.jar $hadoop_home/common/*.jar $hadoop_home/common/lib/*.jar"
hdfs_classpath=`echo $hdfs_classpath | sed "s/ /:/g"`
for ((i=0;i<n_nodes;i++))
do
#      ssh "${hosts[i % n_hosts]}" "cd /srv/remote/all-common/tpch/data ; ulimit -c unlimited ; export LD_LIBRARY_PATH=. ; ./tpch -cache -tmp ~ -inmem-threshold 10000 $i $n_nodes $nodes" > node$i.log 2>&1 &
	ssh "${hosts[i % n_hosts]}" "cd /srv/remote/all-common/tpch/data ; ulimit -c unlimited ; export CLASSPATH=${hdfs_classpath} ; export LD_LIBRARY_PATH=. ; ./tpch -dir hdfs://strong:9121 -format parquet -tmp ~ -inmem-threshold 100000000 $i $n_nodes $nodes" > node$i.log 2>&1 &
done
wait
