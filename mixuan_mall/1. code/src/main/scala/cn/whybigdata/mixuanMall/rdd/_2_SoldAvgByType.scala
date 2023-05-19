package cn.whybigdata.mixuanMall.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author：whybigdata
 * @date：Created in 21:19 2023-05-09 
 * @description：求各种类型商品的平均已售量
 */
object _2_SoldAvgByType {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("AvgScore")
        val sc: SparkContext = new SparkContext(conf)

//        val dataFile = "dataset/test/test.txt"
        val dataFile = "dataset/input/tb_mx_mall.txt"
        val data: RDD[String] = sc.textFile(dataFile,4)

        val res: RDD[(String, Int)] = data
          .filter(_.trim().length > 0)
          // 1 -> type  6 -> sold
          .map(line => (line.split("\t")(1).trim(), line.split("\t")(6).trim().replace("\"", "").toInt))
          .partitionBy(new HashPartitioner(1))
          .groupByKey()
          .map(x => {
              var n = 0
              var sum = 0.0
              for (i <- x._2) {
                  sum = sum + i
                  n = n + 1
              }
              val avg = sum / n
              val format = f"$avg%1.2f".toDouble
              // 已售量采用【不保留小数点】的方式
              val avgStr: String = format.toString
              val index: Int = avgStr.indexOf('.')
              (x._1, avgStr.substring(0, index).toInt)
          })

        res.saveAsTextFile("dataset/out/out-rdd02")
//        res.foreach(println)
    }

}
