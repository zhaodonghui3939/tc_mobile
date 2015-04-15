package org.com.tianchi.data.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
//随机森林注释测试1
class RandomForest(data:RDD[LabeledPoint]) {
  def run = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 50 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32
    RandomForest.trainRegressor(data, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  }
}
