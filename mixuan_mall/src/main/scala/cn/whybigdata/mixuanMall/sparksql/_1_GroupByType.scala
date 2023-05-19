package cn.whybigdata.mixuanMall.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author：whybigdata
 * @date：Created in 10:53 2023-05-10 
 * @description：
 */
object _1_GroupByType {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("GroupByType")
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

        //df.groupBy("type").count().show()
		df.createOrReplaceTempView("tb_mx_mall")

        /**
         * +--------+----+
         * |    type| cnt|
         * +--------+----+
         * |    生鲜|3278|
         * |运动户外|3031|
         * |    美妆|5634|
         * |  3C数码|3372|
         * +--------+----+
         */
        spark.sql("select type, count(*) as cnt from tb_mx_mall group by type").show()

    }
}
