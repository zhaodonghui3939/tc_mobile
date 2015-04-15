package org.com.tianchi.data.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.com.tianchi.data.global.Para
class GBRT(data:RDD[LabeledPoint]) {
  def run()={
    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(Para.gbt_num_iteration)
    boostingStrategy.treeStrategy.setMaxDepth(Para.gbt_tree_max_depth)

    GradientBoostedTrees.train(data, boostingStrategy)
  }
}
