package org.com.tianchi.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
//随机森林用regression的方法
class RF(data:RDD[LabeledPoint]) {
  def run = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 400 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 6
    val maxBins = 32
    RandomForest.trainRegressor(data, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  }
}
