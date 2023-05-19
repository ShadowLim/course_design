package cn.whybigdata.mixuanMall.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.OutputMode

/**
 * @author：whybigdata
 * @date：Created in 13:05 2023-05-11 
 * @description：统计各家店铺商品数量
 */
object _2_StructuredStreaming {
    def main(args: Array[String]): Unit = {

        val sparkSession: SparkSession = SparkSession.builder()
          .master("local[2]")
          .appName("StructedStreaming")
//          .config("spark.sql.streaming.maxRowsInConsole", 1000)  // 设置最大输出行数 默认为20行
          .getOrCreate()

        import sparkSession.implicits._
        // TODO  csv kafka socket json text
        // 1) socket
//        val source: Dataset[String] = sparkSession.readStream
//          .format("socket")
//          .options(Map(
//              "host" -> "localhost",
//              "port" -> "9999",
//          )).load().as[String]

        // todo 2) kafka
//        val source: Dataset[String] = sparkSession.readStream
//          .format("kafka")
//          .option("kafka.bootstrap.servers", "192.168.149.137:9092")
//          .option("subscribe", "tp-mx-mall")
//          .load().as[String]

        // todo 3) csv
//        val source: Dataset[String] = sparkSession.readStream
//          .format("csv")
//          .option("header", "true")   // 是否包含 CSV 表头
//          .option("delimiter", ",")   // 分隔符
//          .option("path", "/data/csv")  // 数据文件目录
//          .schema(mySchema)          // 指定 schema，如果 CSV 文件没有表头，可以在这里定义每一列的名字和类型，例如 StructField("name", StringType)
//          .load()

        // todo 4) txt 两种方式
//        val source: Dataset[String] = sparkSession.readStream
//          .format("text")
//          .option("path", "dataset/input2")
//          .load().as[String]

           val source: Dataset[String] = sparkSession.readStream
              .textFile("dataset/input2")


        // 输出
        val res: DataFrame = source
          .filter(line => line.split("\t").size >= 11)
          .map(line => line.split("\t")(10).replace("\"", ""))
          .groupBy($"value")
          .agg(count("*").as("cnt"))
          .orderBy($"cnt".desc)
          .withColumnRenamed("value", "store")


        res.writeStream
          .format("console")
          .outputMode(OutputMode.Complete())
          .start()
          .awaitTermination()

    }
}
