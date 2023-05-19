package cn.whybigdata.mixuanMall.sparksql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author：whybigdata
 * @date：Created in 20:49 2023-05-10 
 * @description：统计各类商品近24小时销售量的最大最小值
 */
object _3_Sales24MaxMinByType {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .appName("Sales24MaxMin")
          .master("local[2]")
          .getOrCreate()

        import spark.implicits._
        val df: DataFrame = spark.read
          .option("header", false)
          .option("sep", ",")
          .csv("dataset/input/tb_mx_mall.csv")
          .withColumnRenamed("_c0", "id")
          .withColumnRenamed("_c1", "type")
          .withColumnRenamed("_c2", "product")
          .withColumnRenamed("_c3", "price")
          .withColumnRenamed("_c4", "commission_rate")
          .withColumnRenamed("_c5", "commission")
          .withColumnRenamed("_c6", "sold")
          .withColumnRenamed("_c7", "endTime")
          .withColumnRenamed("_c8", "sales2")
          .withColumnRenamed("_c9", "sales24")
          .withColumnRenamed("_c10", "store")
          .withColumnRenamed("_c11", "description_match")
          .withColumnRenamed("_c12", "logistics_service")
          .withColumnRenamed("_c13", "service_attitude")
          .withColumnRenamed("_c14", "collection_time")
          // 转换为Integer类型（agg()函数只支持numberic类型）
          .withColumn("sales24",col("sales24").cast(IntegerType))
//        withColumn("sales24",col("sales24").cast("int"))
//        withColumn("sales24",col("sales24").cast("integer"))

        //df.printSchema()

        df.groupBy("type").max("sales24").show()
        df.groupBy("type").min("sales24").show()
		
		/**
         * +--------+-----------+-----------+
         * |    type|sales24_max|sales24_min|
         * +--------+-----------+-----------+
         * |    生鲜|     327000|          1|
         * |运动户外|    1818000|          1|
         * |    美妆|       null|       null|
         * |  3C数码|     152000|          1|
         * +--------+-----------+-----------+
         */
        df.createOrReplaceTempView("tb_mx_mall")
        spark.sql("select type, max(sales24) as sales24_max, min(sales24) as sales24_min " +
          "from tb_mx_mall group by type").show()

    }
}
