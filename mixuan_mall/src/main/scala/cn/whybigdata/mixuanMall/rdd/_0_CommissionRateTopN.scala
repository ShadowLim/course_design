package cn.whybigdata.mixuanMall.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author：whybigdata
 * @date：Created in 20:46 2023-05-09 
 * @description：按照商品佣金率(commission_rate)进行排序,输出前10的商品和类型（id, type,product,commission_rate,commission）
 */
object _0_CommissionRateTopN {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CommissionRateTopN")
        val sc: SparkContext = new SparkContext(conf)

        //val dataFile = "dataset/test/test.txt"
        val dataFile = "dataset/input/tb_mx_mall.txt"
        val data: RDD[String] = sc.textFile(dataFile)
        // TODO  topN -- sortBy + take
        val rddRes: Array[(String, String, String, Double, Double)] = data.map(
            line => (
              // id type product commission_rate commission
              line.split("\t")(0).trim(),
              line.split("\t")(1).trim(),
              line.split("\t")(2).trim(),
              line.split("\t")(4).trim().replace("\"", "")
              .replace("%", "").toDouble,
              line.split("\t")(5).trim().replace("\"", "").toDouble
            )
        ).sortBy(line => line._4, false).take(10)



        for (elem <- rddRes) {
            /*
            ("1494","3C数码","8000款经典怀旧70809000小霸王街机MD任天堂GBA其乐无穷",80.0%,11.94)
            ("1728","3C数码","怀旧经典大作 仙剑安卓版",80.0%,11.94)
            ("1840","3C数码","最新手游端游2k23 新年巨作ios/ipad通用手游",80.0%,17.94)
            ("2420","3C数码","苹果手机套[14/1312]系列Iphone1213 纯色液态硅胶手机保护壳简约",80.0%,0.01)
            ("2450","3C数码","苹果13promax液态硅胶手机保护壳纯色iPhone14/1112防摔xsmax全包",80.0%,0.01)
            ("2465","3C数码","苹果手机壳14/131211pro手机保护套硅胶XXS瞳眼镜头全包超薄防摔",80.0%,0.01)
            ("3028","3C数码","植物大战僵尸经典版本 数十款电脑改版安装教程",80.0%,8.94)
            ("5190","美妆","几舒二裂酵母护肤调理水精华水爽肤水",75.0%,96.0)
            ("5492","美妆","几舒超A三重视黄醇紧致抗皱眼部精华乳",75.0%,141.0)
            ("3838","美妆","冰块丝绒唇釉雾面哑光口红唇釉纯欲显白不易沾杯学生唇泥推荐ins",74.0%,22.13)

             */
            println(elem._1, elem._2, elem._3, elem._4 + "%", elem._5)
//            val line: RDD[Any] = sc.makeRDD(Array(elem._1,
//                elem._2,
//                elem._3,
//                elem._4 + "%",
//                elem._5))
//            val array = Array(elem._1, elem._2, elem._3, elem._4 + "%", elem._5)
//            val value: RDD[Any] = sc.parallelize(array, 1)

        }


        sc.stop()
    }
}
