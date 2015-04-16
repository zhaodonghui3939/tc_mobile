package org.com.tianchi.data.model
import org.apache.spark.mllib.classification.{SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
class SVM(data: RDD[LabeledPoint]) {
  private val dataLog = data.map(line =>
    new LabeledPoint(line.label, Vectors.dense(line.features.toArray.map(line => Math.log(1 + line)))))
  def run = {
    new SVMWithSGD().run(dataLog)
  }
}
