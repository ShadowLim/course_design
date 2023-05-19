import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * @author：whybigdata
 * @date：Created in 17:09 2023-05-13
 * @description：
 */
object FileStream {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("FileStreamTest").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val data: DStream[String] = ssc.textFileStream("dataset/test2")
        val res: DStream[(String, Int)] = data.map(line => (line.split("\t")(0), 1)).reduceByKey(_ + _)
        res.foreachRDD(rdd => rdd.foreach(println))
        ssc.start()
        ssc.awaitTermination()
    }
}

