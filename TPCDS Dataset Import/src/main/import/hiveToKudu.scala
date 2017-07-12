package `import`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * Created by yunchen on 2017/7/4.
  *
  * 提交1：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn --queue root.public kudu-learning.jar --class import.hiveToKudu
  *
  *   如果按照上面提交，大批量数据（大于15亿行的时候）会出现timeout报错，所以改成如下了：
  *
  * 提交2：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn-client --num-executors 30 --executor-memory 8G --executor-cores 2 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=600s --queue root.public --class import.hiveToKudu kudu-learning.jar
  *
  *   上面的driver是在本机，连接断开，程序也退出了，可以提交到集群模式
  *
  * 提交3：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster --num-executors 30 --executor-memory 8G --executor-cores 2 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=600s --queue root.public --class import.hiveToKudu kudu-learning.jar
  *
  *   这种提交方式会报错，找不到表，User class threw exception: org.apache.spark.sql.catalyst.analysis.NoSuchTableException: Table or view 'store_sales' not found in database 'tpcds_bin_partitioned_parquet_1000';
  *   很奇怪，明明表存在，不知道什么原因，哪里可能写错了，暂时未找到
  *
  *   解决了加入--files /etc/hive/conf/hive-site.xml，参考https://community.hortonworks.com/questions/5798/spark-hive-tables-not-found-when-running-in-yarn-c.html
  *
  * 提交4：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --files /etc/hive/conf/hive-site.xml --master yarn --deploy-mode cluster --num-executors 30 --executor-memory 8G --executor-cores 2 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=600s --queue root.public --class import.hiveToKudu kudu-learning.jar
  */
object hiveToKudu {

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("参数必须是三个，如下：")
      println("\t args(0):源表名称数组，逗号分隔 \n" +
        "\t args(1)：源表数据库名 \n" +
        "\t args(2)：目标数据库名，需要先创建好所有表 \n")
    }

    //定义kudu master地址，多个用逗号分隔
    val kuduMaster = "zjdw-pre0069:7051"
    //定义hive源数据库名
    //val srcDatabaseName = "tpcds_bin_partitioned_parquet_1000"

    val srcDatabaseName = args(1)

    //定义hive源表名数组
   /* val arrTableName =  Array[String]("call_center","catalog_page","customer","customer_address","customer_demographics","date_dim","household_demographics"
            ,"income_band","inventory","item","promotion","reason","ship_mode","store","time_dim","warehouse","web_page","web_returns","web_site","catalog_returns"
            ,"store_returns","catalog_sales","web_sales","store_sales")*/
    /*val arrTableName =  Array[String]("customer","customer_address","customer_demographics",
      "date_dim","household_demographics","income_band","inventory","item","promotion","time_dim",
     "warehouse","web_page","reason","ship_mode","store","store_returns","catalog_sales","store_sales")*/

    val tableNameList = args(0)

    val arrTableName = Array[String](tableNameList)
    //定义目标数据库名
    //val dstDatabaseName = "kudu_spark_tpcds_1000"
    val dstDatabaseName = args(2)

    //定义sparksession
    val spark = SparkSession
      .builder()
      .appName("Import Hive Data TO Kudu Use Spark")
      .enableHiveSupport()
      .getOrCreate()

    for ( srcTableName <- arrTableName ) {
      println("当前写入的表是："+srcTableName)
      readHiveToDFToKudu(kuduMaster, dstDatabaseName, srcDatabaseName,srcTableName, spark)
    }

  }

  /**
    *
    * @param kuduMaster kudu master地址
    * @param dstDatabaseName  目标数据库名称
    * @param srcDatabaseName  源数据库名称
    * @param srcTableName 源表名称列表。逗号分隔
    * @param sp sparkSession
    *
    */
  def readHiveToDFToKudu(kuduMaster:String, dstDatabaseName:String, srcDatabaseName:String, srcTableName:String, sp:org.apache.spark.sql.SparkSession) = {

    val dstTableName = srcTableName

    //val kuduContext = new KuduContext("zjdw-pre0069:7051")
    val kuduContext = new KuduContext(kuduMaster)

    import sp.implicits
    val tableDF = sp.read.table(srcDatabaseName+"."+srcTableName)

    ////这里支持insertRows，deleteRows，upsertRows，详细见官网实例：http://kudu.apache.org/docs/developing.html
    kuduContext.upsertRows(tableDF,"impala::"+dstDatabaseName+"."+dstTableName)

    //下面这种方式只支持appent模式,这里.kudu报错，查看过是有那个class的，里面的方法也有，不知道为什么，前面查询都可以
    //tableDF.write.options(Map("kudu.master"-> kuduMaster, "kudu.table"-> "impala::default.spark_kudu")).mode("append").kudu

  }

}






