package org.com.tianchi.data.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.com.tianchi.data.global.Para

/**
 * Created by closure on 15/4/13.
 */
class GBRT(true_training_data: RDD[LabeledPoint]) {
  val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Regression")
  boostingStrategy.setNumIterations(Para.gbt_num_iteration)
  boostingStrategy.treeStrategy.setMaxDepth(Para.gbt_tree_max_depth)
  GradientBoostedTrees.train(true_training_data, boostingStrategy)

}
