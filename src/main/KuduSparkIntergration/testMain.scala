package main.KuduSparkIntergration

/**
  * Created by yunchen on 2017/6/12.
  */
object testMain {

  def main(args: Array[String]): Unit = {
    val aa = "111:111,222:222"
    val bb = Seq(aa).mkString(",")
    println(bb)
  }

}
