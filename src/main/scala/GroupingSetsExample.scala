
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, grouping_id, sum}

/**
 *  Hello Scala
 */
object GroupingSetsExample {
  def main(args: Array[String]):Unit = {
    println("Hello Scala Spark World!")
    val spark = SparkSession.builder().master("local").appName("yifan_spark_test")
      .getOrCreate()
    import spark.implicits._
    //rollup
    val rolledUpDf = Seq(
      ("2020-01-01", "USA", "30"),
      ("2020-01-02", "USA", "70"),
      ("2020-01-01","China","90"))
      .toDF("Date","Country","Quantity")
      .rollup("Date","Country")
      .agg(sum("Quantity"))
      .selectExpr("Date", "Country","`sum(Quantity)` as totalQuantity")
    rolledUpDf.show(false)

    //cube
    val cubedDF = Seq(
      ("2020-01-01", "USA", "30"),
      ("2020-01-02", "USA", "70"),
      ("2020-01-01","China","90"))
      .toDF("Date","Country","Quantity")
      .cube("Date","Country")
      .agg(grouping_id(),sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)
    cubedDF.show(false)
  }

}
