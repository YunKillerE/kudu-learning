# 数据量


# TPC-H 100G数据导入与测试结果

参考下面改改代码就好了，后续会改成传变量的方式

# TPC-DS 1TB数据集导入与测试结果

编译src.main.import，spark提交

```
 提交1：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn --queue root.public kudu-learning.jar --class import.hiveToKudu

   如果按照上面提交，大批量数据（大于15亿行的时候）会出现timeout报错，所以改成如下了：

 提交2：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn-client --num-executors 30 --executor-memory 8G --executor-cores 2 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=600s --queue root.public --class import.hiveToKudu kudu-learning.jar

   上面的driver是在本机，连接断开，程序也退出了，可以提交到集群模式（但是测试报错了）：

 提交3：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster --num-executors 30 --executor-memory 8G --executor-cores 2 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=600s --queue root.public --class import.hiveToKudu kudu-learning.jar

   这种提交方式会报错，找不到表，User class threw exception: org.apache.spark.sql.catalyst.analysis.NoSuchTableException: Table or view 'store_sales' not found in database 'tpcds_bin_partitioned_parquet_1000';
   很奇怪，明明表存在，不知道什么原因，哪里可能写错了，暂时未找到原因
```

# TPC-DS 30TB数据导入与测试结果
























