package cn.whybigdata.mixuanMall.sparksql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

/**
 * @author：whybigdata
 * @date：Created in 21:29 2023-05-10 
 * @description：统计商品数量前10的店铺
 */
object _4_Top10ProductCntBYStore {
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

        val resDF: DataFrame = df.groupBy("type", "store").count()
        //resDF.printSchema()
        // 降序排列取Top10
        val rows: Array[Row] = resDF.orderBy(-resDF("count")).take(10)
        rows.foreach(println)
		
		df.createOrReplaceTempView("tb_mx_mall")

        /**
         * +--------+----------------------+---+
         * |    type|                 store|cnt|
         * +--------+----------------------+---+
         * |  3C数码|                壳比家|472|
         * |  3C数码|              仪宝严选|256|
         * |运动户外|Yvette薏凡特官方旗舰店|115|
         * |    美妆|        再变美妆旗舰店|105|
         * |运动户外|        江和运动服饰店| 97|
         * |    美妆|            ZRZR旗舰店| 88|
         * |运动户外|      斯凯奇泉州专卖店| 68|
         * |运动户外|    法茜运动户外旗舰店| 64|
         * |    美妆|Myonly麦欧丽美妆旗舰店| 63|
         * |    美妆|      倾城美美妆专营店| 62|
         * +--------+----------------------+---+
         */
        spark.sql("select type, store, count(*) as cnt " +
          "from tb_mx_mall group by type, store order by cnt desc limit 10").show()

    }
}
