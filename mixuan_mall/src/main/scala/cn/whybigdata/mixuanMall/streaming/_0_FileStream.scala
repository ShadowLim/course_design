package cn.whybigdata.mixuanMall.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author：whybigdata
 * @date：Created in 08:53 2023-05-12 
 * @description：使用本地文件流统计不同类型商品的数量
 */
object _0_FileStream {
    def main(args: Array[String]): Unit = {
        //设置为本地运行模式，2个线程，一个监听，另一个处理数据
        val sparkConf = new SparkConf().setAppName("_0_FileStream").setMaster("local[2]")

        val ssc = new StreamingContext(sparkConf, Seconds(2))   // 每2秒处理一次数据

        val data: DStream[String] = ssc.textFileStream("dataset/input2")
        val res: DStream[(String, Int)] = data.map(line => (line.split("\t")(1).replace("\"", ""), 1)).reduceByKey(_ + _)
        res.foreachRDD(rdd => rdd.foreach(println))

        ssc.start()
        ssc.awaitTermination()

    }
}
