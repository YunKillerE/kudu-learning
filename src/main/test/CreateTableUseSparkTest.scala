package test

import main.Utils.KuduSparkCRUD
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._

/**
  * Created by yunchen on 2017/6/12.
  */
class CreateTableUseSparkTest {

  def main(args: Array[String]): Unit = {

    val masterList = "cmname1:7051"

    val kuduContext = new KuduContext(masterList)

    CreateTableUseSpark(kuduContext)

  }

  def CreateTableUseSpark(kuduContext:KuduContext): Unit = {


/*    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("s_suppkey", IntegerType , false) ::
        StructField("s_address" , StringType, true ) ::
        StructField("s_nationkey" , IntegerType, true ) ::
        StructField("s_phone" , DoubleType, true ) ::
        StructField("s_acctbal" , StringType, true ) ::
        StructField("s_comment", StringType , true ) :: Nil)*/

/*    /**
      * create table tsgz_stds (
	stds_id STRING,
	stds_srcip STRING,
	stds_dstip STRING,
	stds_rulename STRING,
	stds_timedate STRING,
	PRIMARY KEY (stds_id)
)
PARTITION BY HASH (stds_id) PARTITIONS 8
STORED AS KUDU
      *
      *

/*    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("stds_id", StringType , false) ::
        StructField("stds_srcip" , StringType, true ) ::
        StructField("stds_dstip" , StringType, true ) ::
        StructField("stds_rulename" , StringType, true ) ::
        StructField("stds_timedate" , StringType, true ) :: Nil)
      */*/*/

/*    /**
      * Query: describe tsgz_syslog_new
+--------------+--------+---------+-------------+----------+---------------+---------------+---------------------+------------+
| name         | type   | comment | primary_key | nullable | default_value | encoding      | compression         | block_size |
+--------------+--------+---------+-------------+----------+---------------+---------------+---------------------+------------+
| sys_id       | string |         | true        | false    |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| sys_hostname | string |         | false       | true     |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| sys_time     | string |         | false       | true     |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| sys_message  | string |         | false       | true     |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| sys_user     | string |         | false       | true     |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| sys_srchost  | string |
      */*/
/*
    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("sys_id", StringType , false) ::
        StructField("sys_hostname" , StringType, true ) ::
        StructField("sys_time" , StringType, true ) ::
        StructField("sys_message" , StringType, true ) ::
        StructField("sys_user" , StringType, true ) ::
        StructField("sys_srchost" , StringType, true ) :: Nil)
*/
val kuduTableSchema = StructType(
  //        column name   type       nullable
  StructField("tc_uuid", StringType , false) ::
    StructField("tc_id" , StringType, true ) ::
    StructField("tc_yy" , StringType, true ) ::
    StructField("tc_srcip" , StringType, true ) ::
    StructField("tc_dstip" , StringType, true ) ::
    StructField("tc_protocol" , StringType, true ) ::
    StructField("tc_score" , StringType, true ) ::
    StructField("tc_comment" , StringType, true ) :: Nil)

    val tablename = "impala::default.tsgz_tcdns"

    val kudupri = "tc_uuid"

    val kuducrud = new KuduSparkCRUD(kuduContext)

    kuducrud.CreateTableUseSpark(tablename,true,kuduTableSchema,kudupri,3)
  }

}
