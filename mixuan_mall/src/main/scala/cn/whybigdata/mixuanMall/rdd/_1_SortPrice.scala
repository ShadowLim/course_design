package cn.whybigdata.mixuanMall.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author：whybigdata
 * @date：Created in 20:47 2023-05-09 
 * @description：按照商品销售价格(出售价price)进行排序,输出(id,type,product,price)
 */
object _1_SortPrice {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("_1_Sort")
        val sc: SparkContext = new SparkContext(conf)
//        val dataFile = "dataset/test/test.txt"
        val dataFile = "dataset/input/tb_mx_mall.txt"
        val data: RDD[String] = sc.textFile(dataFile)


        // TODO  转换算子 - sortBy
        val rddRes: RDD[(String, String, String, Double)] = data.map(
            line => (
              line.split("\t")(0).trim(),
                  line.split("\t")(1).trim(),
                  line.split("\t")(2).trim(),
                  line.split("\t")(3).trim().replace("\"", "").toDouble
            )
        ).sortBy(line => line._4, false)

//        println(rddRes.collect.mkString("\n"))
        rddRes.saveAsTextFile("dataset/out/out-rdd01_sort")
        sc.stop()
    }

}
