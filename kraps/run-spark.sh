rm -f nohup.out ; nohup spark-sql --executor-memory 32G --executor-cores 8 --num-executors 4 --driver-memory 8G --master spark://strong:7077 -f spark-tpch-query.sql &
