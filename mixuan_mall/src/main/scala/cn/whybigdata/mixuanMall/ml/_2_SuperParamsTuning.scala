package cn.whybigdata.mixuanMall.ml

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}

/**
 * @author：whybigdata
 * @date：Created in 22:49 2023-05-11 
 * @description：在_0_PriceAndCommentRegression的基础上进行超参数调优
 */
object _2_SuperParamsTuning {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .appName("_1_KMeans")
          .master("local[2]")
          .getOrCreate()

        import spark.implicits._

        // 读取Irisi数据集
        val data: DataFrame = spark.sparkContext.textFile("dataset/input/tb_mx_mall.txt")
          .map(_.split("\t"))
          // (price, description_match, logistics_service, service_attitude) --> type
          .map(
              p => MxMall2(
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

        // 分别获取标签列和特征列，进行索引、重命名，并设置机器学习工作流
        val Array(trainingData,testData) = data.randomSplit(Array(0.7, 0.3))
        val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
        val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(data)
        val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(50)
        val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

        val lrPipeline = new Pipeline().setStages(Array(labelIndexer,featureIndexer, lr, labelConverter))

        // 使用ParamGridBuilder方便构造参数网格
        val paramGrid = new ParamGridBuilder()
          .addGrid(lr.elasticNetParam,Array(0.2, 0.8))
          .addGrid(lr.regParam,Array(0.01, 0.1, 0.5))
          .build()

        // 构建针对整个机器学习工作流的交叉验证类
        // 定义验证模型、参数网格，以及数据集的折叠数，并调用fit方法进行模型训练
        val cv = new CrossValidator().setEstimator(lrPipeline)
          .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel")
          .setPredictionCol("prediction")).setEstimatorParamMaps(paramGrid)
          .setNumFolds(4)   //Use 4+in practice

        val cvModel = cv.fit(trainingData)
        val lrPredictions=cvModel.transform(testData)

        lrPredictions.select("predictedLabel","label", "features", "probability").collect()
          .foreach {
              case Row(predictedLabel: String, label: String, features: Vector, prob: Vector) =>
                  println(s"($label,$features)-->prob=$prob,predicted Label=$predictedLabel")
          }

        val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
        val lrAccuracy = evaluator.evaluate(lrPredictions)

        // 获取最优的逻辑斯蒂回归模型，并查看其具体的参数
        val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
        val lrModel = bestModel.stages(2).asInstanceOf[LogisticRegressionModel]

        /**
         * Coefficients:
         * -6.848927089709129E-5  0.04875656048146965    0.581075496224853    -1.2615121298508152
         * 3.4021602820603794E-4  -0.20969537386711487   0.10793059182541413  0.211205154164158
         * -5.498132861090596E-4  -0.027942481327275153  -0.1774155022289255  0.3400290836327852
         * 7.533533582009425E-5   0.190402302641057      -0.4520173839994679  0.3760998106645164
         * Intercept:[0.4287788071607362,-0.08997225493363671,-0.13910201992277804,-0.19970453230432145]
         * numclasses:4
         * numFeatures:4
         */
        println("Coefficients:\n" + lrModel.coefficientMatrix + "\nIntercept:" +
          lrModel.interceptVector + "\nnumclasses:" + lrModel.numClasses +
          "\nnumFeatures:" + lrModel.numFeatures)

        println("参数网格的最优参数取值为：")

        /**
         * regParam: regularization parameter (>= 0) (default: 0.0, current: 0.01)
         * elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0, current: 0.2)
         */
        println(lrModel.explainParam(lrModel.regParam))
        println(lrModel.explainParam(lrModel.elasticNetParam))

    }
}
case class MxMall2(features:org.apache.spark.ml.linalg.Vector, label:String)