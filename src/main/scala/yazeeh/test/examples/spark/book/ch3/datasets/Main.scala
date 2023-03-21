package yazeeh.test.examples.spark.book.ch3.datasets

import org.apache.spark.sql.{Encoders, SparkSession}
import yazeeh.test.examples.spark.book.ch3.MMs.getSparkConf

import java.util.Base64.Encoder

object Main {

  def main(array: Array[String]) {

    val spark = SparkSession.builder().config(getSparkConf).getOrCreate();
    import spark.implicits._

    //    val deviceIoTData = Encoders.bean(classOf[DeviceIoTData])
    val ds = spark.read
      .format("json")
      .option("path", "src/main/scala/yazeeh/test/examples/spark/book/ch3/datasets/data.json")
      .option("multiLine", true)
      .load()
      .as[DeviceIoTData]

    ds.show(5, false)

    val mconf = spark.conf.getAll
    for (k <- mconf.keySet) {
      println(s"${k} -> ${mconf(k)}\n")
    }


  }

}
