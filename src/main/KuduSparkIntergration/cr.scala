package src.main.KuduSparkIntergration

import org.apache.kudu.spark.kudu.KuduContext
import test.CreateTableUseSparkTest

/**
  * Created by yunchen on 2017/8/30.
  */
object cr {
  def main(args: Array[String]): Unit = {

    val masterList = "cmname1:7051"
    val kuduContext = new KuduContext(masterList)

    val ct = new CreateTableUseSparkTest()

    //创建表，这里创建的表在impala里面看不到
    ct.CreateTableUseSpark(kuduContext)

  }


}
