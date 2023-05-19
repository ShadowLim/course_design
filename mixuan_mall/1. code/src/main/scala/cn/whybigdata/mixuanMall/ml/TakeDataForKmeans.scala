package cn.whybigdata.mixuanMall.ml

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author：whybigdata
 * @date：Created in 10:48 2023-05-11 
 * @description：
 */
object TakeDataForKmeans {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .appName("TakeDataForKmeans")
          .master("local[1]")
          .getOrCreate()

        import spark.implicits._

        val rawData: RDD[String] = spark.sparkContext.textFile("dataset/input/tb_mx_mall.txt")

        val takeData: DataFrame = rawData.map(_.split("\t"))
          .map(
              // field: price commission sold sales2 sales24 -> index: 3 5 6 8 9
              line => Array(
                  line(3).replace("\"", "").toDouble,
                  if (line(5).toString() == "") {
                      0.0
                  } else {
                      line(5).replace("\"", "").toDouble
                  },
                  if (line(6).toString() == "") {
                      0
                  } else {
                      line(6).replace("\"", "").toInt
                  },
                  if (line(8).toString() == "") {
                      0
                  } else {
                      line(8).replace("\"", "").toInt
                  },
                  if (line(9).toString() == "") {
                      0
                  } else {
                      line(9).replace("\"", "").toInt
                  }
              )
          ).toDF()
        //takeData.show(30, false)
        val takeRDD: RDD[Row] = takeData.rdd
        //takeRDD.collect().foreach(println)

        // RDD[Row] 类型 转换为 RDD[String] 类型
        val strRDD: RDD[String] = takeRDD.map(_.mkString(","))
        /**
         * [WrappedArray(39.9, 11.97, 238000.0, 17000.0, 67000.0)]
         * [WrappedArray(9.9, 5.64, 266000.0, 18000.0, 118000.0)]
         * [WrappedArray(99.0, 19.8, 61000.0, 0.0, 0.0)]
         * [WrappedArray(299.0, 62.79, 11000.0, 1496.0, 5802.0)]
         * [WrappedArray(198.0, 39.6, 8678.0, 0.0, 0.0)]
         * [WrappedArray(69.0, 20.7, 38000.0, 4178.0, 16000.0)]
         * [WrappedArray(138.0, 41.4, 17000.0, 1540.0, 7272.0)]
         * [WrappedArray(99.8, 19.96, 20000.0, 0.0, 0.0)]
         * [WrappedArray(12.8, 3.84, 327000.0, 19000.0, 79000.0)]
         * [WrappedArray(6.99, 1.96, 303000.0, 21000.0, 152000.0)]
         * [WrappedArray(19.8, 4.95, 973000.0, 13000.0, 0.0)]
         * ..............................
         */
        //strRDD.collect().foreach(println)

        strRDD.saveAsTextFile("dataset/out/ml/kmeans-pre")
    }

}
