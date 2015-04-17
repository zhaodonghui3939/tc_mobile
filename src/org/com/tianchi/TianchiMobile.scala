package org.com.tianchi

import org.apache.spark.SparkContext
import org.com.tianchi.base.BaseComputing
import org.com.tianchi.feature.{UserItemGeohash, ItemFeatures, UserFeatures, UserItemFeatures}
import org.com.tianchi.global.Para
import org.com.tianchi.model.{GBRT, LR, RandomForest, SVM}
import org.com.tianchi.sample.SampleBase

object TianchiMobile {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val data_user = sc.textFile(Para.path_data_user).filter(!_.contains("user_id")).cache()
    val data_item = sc.textFile(Para.path_data_item).filter(!_.contains("item_id"))
    val data_item_real = BaseComputing.getItemSet(data_item)
    //用户对商品的行为集合，按照时间排序 计算方便
    val data_feature_user_item = BaseComputing.getUserItemData(data_user).cache()
    val data_geohash = BaseComputing.getItemGeoHash(data_item).cache()
    /*构造训练集*/
    //用户的行为集合
    val data_feature_user = BaseComputing.getUserData(data_user)
    //商品的行为集合
    val data_feature_item = BaseComputing.getItemData(data_user)
    //类目的行为集合
    val data_feature_category = BaseComputing.getCategoryData(data_user)

    //训练集特征构造和测试
    val feature_user_item = new UserItemFeatures(data_feature_user_item, Para.train_start_date, Para.train_end_date).run().cache()
    //测试地理位置特征
    val user_item_geohash = new UserItemGeohash(data_feature_user_item,data_geohash,
      Para.train_start_date, Para.train_end_date).getUserItemGeoFeatures()




    //计算商品特征集
    val feature_item = new ItemFeatures(data_feature_item, Para.train_start_date, Para.train_end_date).run().cache()
    //计算用户特征集
    val feature_user = new UserFeatures(data_feature_user, Para.train_start_date, Para.train_end_date).run().cache()
    val join_features = BaseComputing.join(feature_user_item, feature_item, feature_user).cache() //特征进行join
    val label_item = BaseComputing.getBuyLabel(data_user, Para.train_label_date) //获取12月17号的标签
    val feature = BaseComputing.toLablePoint(join_features, label_item) //获取标签数据
    //采样训练
    val sample = SampleBase.globalSample(feature, Para.neg_to_pos_rate).cache()
    //不同模型测试
    val model_lbfgs = new LR(sample).runLBFGS
    val model_svm = new SVM(sample).run
    val model_gbrt = new GBRT(sample).run
    val model_rf = new RandomForest(sample).run

    val featuresS = BaseComputing.getSelectFeatureData(feature, data_item_real).cache()
    //测试逻辑回归
    val predict = BaseComputing.lrPredict(featuresS, model_lbfgs, 600) //逻辑回归预测
    val f_lr = BaseComputing.calFvalue(predict, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息
    //测试svm的性能
    val predict_svm = BaseComputing.svmPredict(featuresS, model_svm, 600)
    val f_svm = BaseComputing.calFvalue(predict_svm, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息
    //测试gbrt的性能
    val predict_gbrt = BaseComputing.gbrtPredict(featuresS, model_gbrt, 600)
    val f_gbrt = BaseComputing.calFvalue(predict_gbrt, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息
    //测试rf的性能
    val predict_rf = BaseComputing.rfPredict(featuresS, model_rf, 600)
    val f_rf = BaseComputing.calFvalue(predict_rf, label_item.filter(line => data_item_real.contains(line.split("_")(1)))) //计算f值相关信息

    //测试集特征构造和测试
    val test_feature_user_item = new UserItemFeatures(data_feature_user_item, Para.test_start_date, Para.test_end_date).run().cache()
    val test_feature_item = new ItemFeatures(data_feature_item, Para.test_start_date, Para.test_end_date).run().cache()
    val test_feature_user = new UserFeatures(data_feature_user, Para.test_start_date, Para.test_end_date).run().cache()
    val test_join_features = BaseComputing.join(test_feature_user_item, test_feature_item, test_feature_user).cache() //特征进行join
    val test_label_item = BaseComputing.getBuyLabel(data_user, "2014-12-18") //获取12月18号的标签
    val test_feature = BaseComputing.toLablePoint(test_join_features, test_label_item) //获取标签数据

    //测试集合
    val test_featuresS = BaseComputing.getSelectFeatureData(test_feature, data_item_real).cache()
    //测试逻辑回归
    val test_predict = BaseComputing.lrPredict(test_featuresS, model_lbfgs, 600)
    val test_f = BaseComputing.calFvalue(test_predict, test_label_item.filter(line => data_item_real.contains(line.split("_")(1))))
    //测试
    val test_predict_rf = BaseComputing.rfPredict(test_featuresS, model_rf, 500)
    val test_rf = BaseComputing.calFvalue(test_predict_rf, test_label_item.filter(line => data_item_real.contains(line.split("_")(1))))
  }
}
