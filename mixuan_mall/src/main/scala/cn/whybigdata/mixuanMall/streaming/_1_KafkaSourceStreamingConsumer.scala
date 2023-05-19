package cn.whybigdata.mixuanMall.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author：whybigdata
 * @date：Created in 12:29 2023-05-15
 * @description：求不同类型商品评价3个指标评分的均值
 *   TODO 启动zk kafka spark
 */
object _1_KafkaSourceStreamingConsumer {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("KafkaSourceStreamingConsumer").setMaster("spark://bd01:7077")
        val ssc = new StreamingContext(conf, Seconds(2))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "bd01:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "g-mx-mall",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean),
        )
        val topics = Set("tp-mx-mall")
        val defaultValue : String = "0.0"

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )

        // 获取需要分析的数据（商品类型，商品描述，物流服务，服务态度）
        val originalStream: DStream[(String, String, String, String)] = kafkaStream.map(line => line.toString().split("\t"))
          .map(ratings => (ratings(1), ratings(11).toString().replace("\"", ""),
            ratings(12).toString().replace("\"", ""),
            ratings(13).toString().replace("\"", "")
          ))
        // 提前处理（商品描述，物流服务，服务态度）字段为空的情况
        val updatedStream: DStream[(String, String, String, String)] = originalStream.map {
            case (key, value1, value2, value3) =>
                val updatedValue1 = if (value1.isEmpty) defaultValue else value1
                val updatedValue2 = if (value2.isEmpty) defaultValue else value2
                val updatedValue3 = if (value3.isEmpty) defaultValue else value3
                (key, updatedValue1, updatedValue2, updatedValue3)
        }
        // 核心逻辑
        val res: DStream[(String, (Double, Double, Double))] = updatedStream.map(rating => (rating._1, (
          rating._2.replace("\"", "").toDouble,
          rating._3.replace("\"", "").toDouble,
          rating._4.replace("\"", "").toDouble
        )))
          .groupByKey()
          .mapValues(ratings => {
              //              val count = ratings.size
              //              val sum1 = ratings.map(_._1).sum
              //              val sum2 = ratings.map(_._2).sum
              //              val sum3 = ratings.map(_._3).sum
              //              (sum1 / count, sum2 / count, sum3 / count)
              val sum = ratings.reduce(
                  (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)
              )
              (sum._1 / ratings.size, sum._2 / ratings.size, sum._3 / ratings.size)
          })

//        val ratings: DStream[(String, (Double, Double, Double))] = kafkaStream.map(line => line.toString().split("\t"))
//          .map(rating => (rating(1), (
//            if (rating.nonEmpty) rating(11).toString().replace("\"", "").toDouble else defaultValue,
//            if (rating.nonEmpty) rating(12).toString().replace("\"", "").toDouble else defaultValue,
//            if (rating.nonEmpty) rating(13).toString().replace("\"", "").toDouble else defaultValue
//          )))
//          .groupByKey()
//          .mapValues(ratings => {
//              //              val count = ratings.size
//              //              val sum1 = ratings.map(_._1).sum
//              //              val sum2 = ratings.map(_._2).sum
//              //              val sum3 = ratings.map(_._3).sum
//              //              (sum1 / count, sum2 / count, sum3 / count)
//              val sum = ratings.reduce(
//                  (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)
//              )
//              (sum._1 / ratings.size, sum._2 / ratings.size, sum._3 / ratings.size)
//          })

        println("---------------------------------------------------------")
        res.foreachRDD(rdd => rdd.foreach(println))

        res.foreachRDD(rdd => rdd.saveAsTextFile("file:///output/kfk_stream.out"))
        println("*********************************************************")

        ssc.start()
        ssc.awaitTermination()
    }
}
