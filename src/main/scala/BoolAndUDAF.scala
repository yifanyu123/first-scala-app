import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{BooleanType, DataType, StructField, StructType}

/**
 * 自定义聚合函数演示
 *
 * @author yuyifan
 */
class BoolAndUDAF extends UserDefinedAggregateFunction{
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", BooleanType) :: Nil)
  def bufferSchema: StructType = StructType(
    StructField("result", BooleanType) :: Nil
  )
  def dataType: DataType = BooleanType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = true
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
  }
  def evaluate(buffer: Row): Any = {
    buffer(0)
  }

}

object BoolAndUDAF {
  def main(args: Array[String]):Unit = {
    val ba = new BoolAndUDAF
    val spark = SparkSession.builder().master("local").appName("yifan_spark_test2")
      .getOrCreate()
    spark.udf.register("booland", ba)
    spark.range(1)
      .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
      .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
      .select(ba(col("t")), expr("booland(f)"))
      .show()
  }
}
