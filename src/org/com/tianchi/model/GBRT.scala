package org.com.tianchi.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.com.tianchi.global.Para

//这里用regression的模型，分类无法获得权值
class GBRT(data: RDD[LabeledPoint]) {
  def run = {
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(Para.gbt_num_iteration)
    boostingStrategy.treeStrategy.setMaxDepth(Para.gbt_tree_max_depth)
    GradientBoostedTrees.train(data, boostingStrategy)
  }
}
