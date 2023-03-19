package yazeeh.test.examples

import org.apache.spark.sql.types._


object Test {

  //define scheme
  val schema = StructType(
    Array(
      StructField("author", StringType, false),
      StructField("title", StringType, false),
      StructField("pages", IntegerType, false)
    )
  )

//  Defining the same schema using DDL is much simpler:
  val ddlSchema = "author STRING, title STRING, pages INT"
}
