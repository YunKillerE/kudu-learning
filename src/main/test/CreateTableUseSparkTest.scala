package test

import main.Utils.KuduSparkCRUD
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._

/**
  * Created by yunchen on 2017/6/12.
  */
object CreateTableUseSparkTest {


  def main(args: Array[String]): Unit = {

    val masterList = "zjdw-pre0069:7051"

    val kuduContext = new KuduContext(masterList)

    CreateTableUseSpark(kuduContext)

  }

  def CreateTableUseSpark(kuduContext:KuduContext): Unit = {

    val tablename = "impala::default.spark_yunchen_supple"

    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("s_suppkey", IntegerType , false) ::
        StructField("s_address" , StringType, true ) ::
        StructField("s_nationkey" , IntegerType, true ) ::
        StructField("s_phone" , DoubleType, true ) ::
        StructField("s_acctbal" , StringType, true ) ::
        StructField("s_comment", StringType , true ) :: Nil)

    val kudupri = "s_suppkey"

    val kuducrud = new KuduSparkCRUD(kuduContext)

    kuducrud.CreateTableUseSpark(tablename,true,kuduTableSchema,kudupri,3)
  }

}
