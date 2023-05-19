package cn.whybigdata.mixuanMall.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author：whybigdata
 * @date：Created in 10:21 2023-05-10 
 * @description：
 */
object _0_SortByEndTime {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("SortByEndTime")
          .master("local[2]")
          .getOrCreate()

        val df: DataFrame = spark.read
          .option("header", false)
          .option("sep", ",")
          .csv("dataset/test/test.csv")
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

        //df.show()
        //df.sort(df("endTime").asc).show()
		df.createOrReplaceTempView("tb_mx_mall")

        /**
         * +-----+--------+-------------------------------------+-----+---------------+----------+------+----------+------+-------+------------------------+-----------------+-----------------+----------------+--------------------+
         * |   id|    type|                              product|price|commission_rate|commission|  sold|   endTime|sales2|sales24|                   store|description_match|logistics_service|service_attitude|     collection_time|
         * +-----+--------+-------------------------------------+-----+---------------+----------+------+----------+------+-------+------------------------+-----------------+-----------------+----------------+--------------------+
         * | 9593|    生鲜|鹿鲜森四川高山甜枇杷果新鲜水果早钟...| 21.9|         15.00%|      3.28|  2556|2023-05-06|  null|   null|                  鹿鲜生|             4.58|             4.98|            4.58|2023-4-18 21:48:3...|
         * |    2|  3C数码|       【9.9元100根】尼龙扎带 松紧...|  9.9|         57.00%|      5.64|266000|2023-06-04| 18000| 118000|                妍宴百货|             4.99|             4.94|             4.9|2023-4-18 20:17:3...|
         * | 5596|    美妆|      veecci唯资眼线胶笔防水不易晕...| 39.7|         35.00%|      13.9|    37|2023-06-30|  null|   null|    VEECCI唯資官方旗舰店|             4.71|             4.73|            null|2023-4-18 21:47:1...|
         * |15311|运动户外| 一次性便捷式马桶垫 旅游出差酒店厕...|  7.9|         52.00%|      4.11|     3|2023-07-31|  null|   null|                鑫禧严选|             4.27|             4.83|            4.66|2023-4-18 21:23:5...|
         * |15312|运动户外|可爱简笔漫画图案休闲帆布包包单肩学...|  9.9|         15.00%|      1.48|  5455|2023-08-31|  null|   null|          喜番优选供应链|             4.47|             4.64|            4.04|2023-4-18 21:23:5...|
         * |    1|  3C数码|【靓点拍】蓝牙自拍杆多功能手机支架...| 39.9|         30.00%|     11.97|238000|2023-12-01| 17000|  67000|        靓点拍数码旗舰店|             4.96|             4.93|             4.8|2023-4-18 20:17:3...|
         * | 9594|    生鲜|       荷美尔经典香煎培根120g*6包 ...|   86|         20.00%|      17.2|  3160|2023-12-28|  null|   null|  荷美尔Hormel官方旗舰店|             4.78|              4.5|            4.94|2023-4-18 21:48:3...|
         * | 5595|    美妆|   单眼皮肿眼泡救星！三年用量！240...| 13.5|         30.00%|      4.05|  1793|2024-02-01|  null|   null|大眼睛女孩美妆工具旗舰店|             4.66|             4.72|            null|2023-4-18 21:47:1...|
         * +-----+--------+-------------------------------------+-----+---------------+----------+------+----------+------+-------+------------------------+-----------------+-----------------+----------------+--------------------+
         */
        spark.sql("select * from tb_mx_mall order by endTime asc").show()
    }
}
