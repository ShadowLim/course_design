package cn.whybigdata.mixuanMall.ml

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.classification.LogisticRegression



/**
 * @author：whybigdata
 * @date：Created in 22:34 2023-05-10 
 * @description： 用二项逻辑斯蒂回归来解决二分类问题（商品类型、商品价格、商品评价）
 */
object _0_PriceAndCommentRegression {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .appName("PriceAndCommentRegression")
          .master("local[2]")
          .getOrCreate()

        import spark.implicits._
        val data: DataFrame = spark.sparkContext.textFile("dataset/input/tb_mx_mall.txt")
          .map(_.split("\t"))
          // (price, description_match, logistics_service, service_attitude) --> type
          .map(
              p => MxMall(
                  Vectors.dense(
                      p(3).replace("\"", "").toDouble,
                      if (p(11).toString() == "") {
                          0.0
                      } else {
                          p(11).replace("\"", "").toDouble
                      },
                      if (p(12).toString() == "") {
                          0.0
                      } else {
                          p(12).replace("\"", "").toDouble
                      },
                      if (p(13).toString() == "") {
                          0.0
                      } else {
                          p(13).replace("\"", "").toDouble
                      }
                  ),
                  p(1).toString()
              )
          ).toDF()
        println("数据集展示：")
        data.show(30, false)

        // 将数据(price, description_match, logistics_service, service_attitude ,type)注册成一个表mx_mall
        data.createOrReplaceTempView("mx_mall")
        val df = spark.sql("select * from mx_mall where label != '美妆'")
        // 获取sql查询结果
        println("sql查询结果如下：")
        df.map(t => t(1) + ":" + t(0)).collect().foreach(println)

        // 分别获取标签列和特征列，进行索引，并进行重命名
        val labelIndexer: StringIndexerModel = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
        val featureIndexer: VectorIndexerModel = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(df)

        // 把数据集随机分成训练集和测试集，其中训练集占70%
        val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

        // 设置logistic的参数：设置循环次数为10次，正则化项为0.3等
        val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

        // 设置一个labelConverter，目的是把预测的类别转化成字符型
        val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

        // 构建pipeline，设置stage，然后调用fit()来训练模型
        val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
        val lrPipelineModel = lrPipeline.fit(trainingData)

        // 利用训练得到的模型对测试集进行验证
        val lrPredictions = lrPipelineModel.transform(testData)

        // 输出预测的结果
        lrPredictions.select("predictedLabel", "label", "features", "probability")
          .collect()
          .foreach {
            case Row(predictedLabel: String, label: String, features: Vector, prob: Vector) =>
                println(s"($label, $features) --> prob=$prob, predicted Label=$predictedLabel")
          }

        // 模型评估
        val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
        val lrAccuracy = evaluator.evaluate(lrPredictions)

        println("Test Error = " + (1.0 - lrAccuracy))

        // 获取逻辑斯蒂模型
        val lrModel = lrPipelineModel.stages(2).asInstanceOf[LogisticRegressionModel]

        println("Coefficients: " + lrModel.coefficientMatrix + "Intercept: "+ lrModel.interceptVector + "numClasses: " +
          lrModel.numClasses + "numFeatures: " + lrModel.numFeatures)

    }

}

case class MxMall(features: org.apache.spark.ml.linalg.Vector, label: String)
