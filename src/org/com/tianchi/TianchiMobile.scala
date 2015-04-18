package org.com.tianchi

import org.apache.spark.SparkContext
import org.com.tianchi.base.BaseComputing
import org.com.tianchi.global.Para
import org.com.tianchi.model.{GBRT, LR, RF, SVM}
import org.com.tianchi.sample.SampleBase

import scala.collection.immutable.HashSet

object TianchiMobile {
  //noinspection ComparingUnrelatedTypes
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val data_user = sc.textFile(Para.path_data_user).filter(!_.contains("user_id")).filter(!_.split(",")(5).split(" ")(0).equals("2014-12-12")).cache()
    val data_item = sc.textFile(Para.path_data_item).filter(!_.contains("item_id"))
    val data_item_real = BaseComputing.getItemSet(data_item)

    val join_features = BaseComputing.createFeatureVector(data_user, data_item, Para.train_start_date, Para.train_end_date)
    val label_item = BaseComputing.getBuyLabel(data_user, Para.train_label_date) //获取12月17号的标签
    val feature = BaseComputing.toLablePoint(join_features, label_item) //获取标签数据
    //采样训练
    val sample = SampleBase.globalSample(feature, Para.neg_to_pos_rate).cache()
    //不同模型测试
    val model_lbfgs = new LR(sample).runLBFGS
    val model_svm = new SVM(sample).run
    val model_gbrt = new GBRT(sample).run
    val model_rf = new RF(sample).run

    val featuresS = BaseComputing.getSelectFeatureData(feature, data_item_real).cache()
    //测试逻辑回归
    val predict = BaseComputing.lrPredict(featuresS, model_lbfgs, Para.result_number) //逻辑回归预测
    val f_lr = BaseComputing.calFvalue(predict, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息
    //测试svm的性能
    val predict_svm = BaseComputing.svmPredict(featuresS, model_svm, Para.result_number)
    val f_svm = BaseComputing.calFvalue(predict_svm, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息
    //测试gbrt的性能
    val predict_gbrt = BaseComputing.gbrtPredict(featuresS, model_gbrt, Para.result_number)
    val f_gbrt = BaseComputing.calFvalue(predict_gbrt, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息
    //测试rf的性能
    val predict_rf = BaseComputing.rfPredict(featuresS, model_rf, Para.result_number)
    val f_rf = BaseComputing.calFvalue(predict_rf, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息

    //测试集特征构造和测试
    val test_join_features = BaseComputing.createFeatureVector(data_user, data_item, Para.test_start_date, Para.test_end_date)
    val test_label_item = BaseComputing.getBuyLabel(data_user, Para.test_label_date) //获取12月18号的标签
    val test_feature = BaseComputing.toLablePoint(test_join_features, test_label_item) //获取标签数据
    //测试集合
    val test_featuresS = BaseComputing.getSelectFeatureData(test_feature, data_item_real).cache()
    //测试逻辑回归
    val test_predict = BaseComputing.lrPredict(test_featuresS, model_lbfgs, Para.result_number)
    val test_f = BaseComputing.calFvalue(test_predict, test_label_item.filter(line => data_item_real.contains(line.split("_")(1))))
    //测试rf
    val test_predict_rf = BaseComputing.rfPredict(test_featuresS, model_rf, Para.rf_result_number)
    val test_rf = BaseComputing.calFvalue(test_predict_rf, test_label_item.filter(line => data_item_real.contains(line.split("_")(1))))

    //预测真实
    val real_join_features = BaseComputing.createFeatureVector(data_user, data_item, Para.real_start_date, Para.real_end_date)
    val real_feature = BaseComputing.toLablePoint(real_join_features, new HashSet[String]) //获取标签数据
    val real_featuresS = BaseComputing.getSelectFeatureData(real_feature, data_item_real).cache()
    //测试逻辑回归
    val real_predict_lr = BaseComputing.lrPredict(real_featuresS, model_lbfgs, Para.result_number)
    val real_predict_rf = BaseComputing.rfPredict(real_featuresS, model_rf, Para.result_number)
    val real_result = BaseComputing.getPredictResult(real_predict_rf)

  }
}
