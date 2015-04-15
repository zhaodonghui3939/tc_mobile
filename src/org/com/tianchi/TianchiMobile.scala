package org.com.tianchi.data

import org.apache.spark.SparkContext
import org.com.tianchi.data.base.BaseComputing
import org.com.tianchi.data.feature.{UserItemFeatures, UserFeatures, ItemFeatures}
import org.com.tianchi.data.global.Para
import org.com.tianchi.data.model.LR
import org.com.tianchi.data.sample.SampleBase

object TianchiMobile {

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val data_user = sc.textFile(Para.path_data_user).filter(!_.contains("user_id")).cache()
    val data_item = sc.textFile(Para.path_data_item).filter(!_.contains("item_id"))
    val data_item_real = BaseComputing.getItemSet(data_item);
    //用户对商品的行为集合，按照时间排序 计算方便
    val data_feature_user_item = BaseComputing.getUserItemData(data_user)
    /*构造训练集*/
    //用户的行为集合
    val data_feature_user = BaseComputing.getUserData(data_user)
    //商品的行为集合
    val data_feature_item = BaseComputing.getItemData(data_user)
    //类目的行为集合
    val data_feature_category = BaseComputing.getCategoryData(data_user)

    //训练集特征构造和测试
    val feature_user_item = new UserItemFeatures(data_feature_user_item,Para.train_start_date,Para.train_end_date).run().cache()
    //计算商品特征集
    val feature_item = new ItemFeatures(data_feature_item,Para.train_start_date,Para.train_end_date).run().cache()
    //计算用户特征集
    val feature_user = new UserFeatures(data_feature_user,Para.train_start_date,Para.train_end_date).run().cache()
    val join_features = BaseComputing.join(feature_user_item,feature_item,feature_user).cache() //特征进行join
    val label_item = BaseComputing.getBuyLabel(data_user,"2014-12-17") //获取12月17号的标签
    val feature = BaseComputing.toLablePoint(join_features,label_item) //获取标签数据
    //采样训练
    val sample = SampleBase.specifySample(feature,data_item_real,20).repartition(6).cache()
    val model_lbfgs = new LR(sample).runLBFGS;
    val featuresS = BaseComputing.getSelectFeatureData(feature,data_item_real).cache()
    val predict = BaseComputing.lrPredict(featuresS,model_lbfgs,0.95)
    val f = BaseComputing.calFvalue(predict,label_item.filter(line => data_item_real.contains(line.split("_")(1))))

    //测试集特征构造和测试
    val test_feature_user_item = new UserItemFeatures(data_feature_user_item,Para.test_start_date,Para.test_end_date).run().cache()
    val test_feature_item = new ItemFeatures(data_feature_item,Para.test_start_date,Para.test_end_date).run().cache()
    val test_feature_user = new UserFeatures(data_feature_user,Para.test_start_date,Para.test_end_date).run().cache()
    val test_join_features = BaseComputing.join(test_feature_user_item,test_feature_item,test_feature_user).cache() //特征进行join
    val test_label_item = BaseComputing.getBuyLabel(data_user,"2014-12-18") //获取12月18号的标签
    val test_feature = BaseComputing.toLablePoint(test_join_features,test_label_item) //获取标签数据

    //预测
    val test_featuresS = BaseComputing.getSelectFeatureData(test_feature,data_item_real).cache()
    val test_predict = BaseComputing.lrPredict(test_featuresS,model_lbfgs,0.987)
    val test_f = BaseComputing.calFvalue(test_predict,label_item.filter(line => data_item_real.contains(line.split("_")(1))))
  }

}
