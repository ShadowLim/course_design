package cn.whybigdata.mixuanMall.sparksql

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @author：whybigdata
 * @date：Created in 11:07 2023-05-10 
 * @description：
 */
object _2_CommentAvgByType {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("CommentAvg")
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

        /**
         * +--------+----------------------+
         * |    type|avg(description_match)|
         * +--------+----------------------+
         * |    生鲜|     4.801912484700075|
         * |运动户外|    4.7900792602377065|
         * |    美妆|     4.733768656716389|
         * |  3C数码|    4.7575385985748895|
         * +--------+----------------------+
         */
        df.groupBy("type").agg("description_match" -> "avg").show()
        /**
         * +--------+----------------------+
         * |    type|avg(logistics_service)|
         * +--------+----------------------+
         * |    生鲜|     4.728534271725801|
         * |运动户外|     4.677916116248317|
         * |    美妆|     4.739953802416452|
         * |  3C数码|     4.741463776722051|
         * +--------+----------------------+
         */
        df.groupBy("type").agg("logistics_service" -> "avg").show()

        /**
         * +--------+---------------------+
         * |    type|avg(service_attitude)|
         * +--------+---------------------+
         * |    生鲜|    4.722839657282701|
         * |运动户外|      4.7097886393659|
         * |    美妆|                 null|
         * |  3C数码|   4.5861045130641775|
         * +--------+---------------------+
         */
        df.groupBy("type").agg("service_attitude" -> "avg").show()
		
		df.createOrReplaceTempView("tb_mx_mall")

        /**
         * +--------+--------------------------------------+-------------------------------------+--------------------------------------+
         * |    type|avg(CAST(description_match AS DOUBLE))|avg(CAST(service_attitude AS DOUBLE))|avg(CAST(logistics_service AS DOUBLE))|
         * +--------+--------------------------------------+-------------------------------------+--------------------------------------+
         * |    生鲜|                     4.801912484700075|                    4.722839657282701|                     4.728534271725801|
         * |运动户外|                    4.7900792602377065|                      4.7097886393659|                     4.677916116248317|
         * |    美妆|                     4.733768656716389|                                 null|                     4.739953802416452|
         * |  3C数码|                    4.7575385985748895|                   4.5861045130641775|                     4.741463776722051|
         * +--------+--------------------------------------+-------------------------------------+--------------------------------------+
         */
        spark.sql("select type, avg(description_match), avg(service_attitude), avg(logistics_service)" +
          "from tb_mx_mall group by type").show()

    }
}
