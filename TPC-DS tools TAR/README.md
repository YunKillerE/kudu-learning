# hive-testbench.tgz

TPC-DS及TPC-H数据集生成工具，可以自己编译，[源码](https://github.com/jimmy-src/hive-testbench)

数据集参考官方文档：http://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v2.5.0.pdf

# kudu-spark_2.10.tgz

执行spark-shell --packages org.apache.kudu:kudu-spark_2.10:1.3.0进入spark shell时需要的包，放到当前用户的/root/.ivy2/cache/org.apache.kudu/kudu-spark_2.10/目录下面就行

注意这里2.10是scala的版本，1.3.0是kudu的版本，我用的是spark1.6，如果是spark2.x改成对应的版本就行