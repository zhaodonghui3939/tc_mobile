package org.com.tianchi.data.model

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Created by closure on 15/4/13.
 */
class LR(data:RDD[LabeledPoint]) {
  //是否有参数调优的可能行
  //进行log话处理
  private val dataLog =  data.map(line =>
    new LabeledPoint(line.label,Vectors.dense(line.features.toArray.map(line => Math.log(1 + line)))))

  def runLBFGS = {
    new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(dataLog)
  }

  def runSGD = {
    new LogisticRegressionWithSGD().run(dataLog)
  }
}
