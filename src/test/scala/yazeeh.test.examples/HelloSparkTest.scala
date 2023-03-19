package yazeeh.test.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import yazeeh.test.examples.HelloSpark.{loadSurveyDF}

class HelloSparkTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = getSparkSession
  }

  test("Data File Loading") {
    val sampleDF = loadSurveyDF("test.csv", spark)
    val rCount = sampleDF.count()
    assert(rCount == 4, "record should be 4")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  private def getSparkSession = {
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate();
    spark
  }


  def getSparkConf: SparkConf = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("Hello Spark")
    sparkConfig.setMaster("local[3]")
    sparkConfig.set("spark.sql.shuffle.partitions", "2")
  }
}
