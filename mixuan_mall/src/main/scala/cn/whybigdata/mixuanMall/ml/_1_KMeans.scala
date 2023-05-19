package cn.whybigdata.mixuanMall.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.tree.loss.SquaredError
import org.apache.spark.rdd.RDD



/**
 * @author：whybigdata
 * @date：Created in 22:46 2023-05-10
 * @description： 用KMeans聚类
 *              https://dblab.xmu.edu.cn/blog/1454/
 *              https://spark.apache.org/docs/3.0.0/ml-clustering.html#k-means
 *              https://dblab.xmu.edu.cn/blog/1288/
 *              RDD Spark-Shell: https://spark.apache.org/docs/3.0.0/mllib-clustering.html
 *
 *              https://www.zhihu.com/tardis/zm/art/265778252?source_id=1005
 */
object _1_KMeans {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .appName("_1_KMeans")
          .master("local[2]")
          .getOrCreate()

        import spark.implicits._

        val rawData: RDD[String] = spark.sparkContext.textFile("dataset/input/kmeans_dataset.txt")

        // 过滤掉类标签
        // 正则表达式 \\d*(\\.?)\\d* 可以用于匹配实数类型的数字，
        // \\d*使用了*限定符，表示匹配0次或多次的数字字符，\\.?使用了?限定符，表示匹配0次或1次的小数点。
        val df: DataFrame = rawData.map(line => {
            model_instance(Vectors.dense(line.split(",")
              .map(line => line.replace("\"", ""))
              .filter(
                  p => p.replace("\"", "") .matches("\\d*(\\" + ".?)\\d*"))
              .map(_.toDouble)
            ))
        }).toDF()

        // 创建Estimator并调用其fit()方法来生成相应的Transformer对象
        val kmeansmodel: KMeansModel = new KMeans()
          .setK(4)
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .fit(df)

        // 使用 transform() 方法对数据集整体处理，生成带有预测簇标签的数据集
        val results: DataFrame = kmeansmodel.transform(df)
        results.collect().foreach(
            row => {
                println(row(0) + " is predicted as cluster " + row(1))
            }
        )

        // 获取到模型的所有聚类中心情况
        kmeansmodel.clusterCenters.foreach(
            center => {
                println("Clustering Center:" + center)
            }
        )

        // 通过计算Silhouette得分来评估聚类情况
        val evaluator = new ClusteringEvaluator()
        val silhouette: Double = evaluator.evaluate(results)
        // todo: Silhouette with squared euclidean distance = 0.988954144321304
        println(s"Silhouette with squared euclidean distance = $silhouette")

        // 计算集合内误差平方和（Within Set Sum of Squared Error, WSSSE)度量聚类的有效性
        // 在真实K值未知的情况下，该值的变化可以作为选取合适K值的一个重要参考
        //kmeansmodel.ClusteringEvaluator(df)
        //org.apache.spark.mllib.tree.loss.SquaredError().computeError


        // 模型保存
        kmeansmodel.save("dataset/out/ml/kmeans")


        // Spark-shell Cluster the data into two classes using KMeans
//        val numClusters = 2
//        val numIterations = 20
//        val clusters = KMeans.train(df, numClusters, numIterations)
//        val WSSSE = clusters.computeCost(df)
//        println(s"Within Set Sum of Squared Errors = $WSSSE")

    }

}

case class model_instance (features: Vector)

