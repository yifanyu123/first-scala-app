import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.storage.StorageLevel

/**
 * RDDExample
 *
 * @author yuyifan
 */
class RDDExample {


}

object RDDExample {
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().master("local").appName("yifan_spark_test")
      .getOrCreate()
    val rdd1 = spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    rdd1.persist(StorageLevel.MEMORY_AND_DISK)
  }
}
