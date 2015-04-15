package org.com.tianchi.data.base
/**
 * Created by closure on 15/4/14.
 * 构造训练数据集
 * 记录由数据结构保存
 */

import org.apache.spark.mllib.classification.{SVMModel, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.com.tianchi.base.Record
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
//一定要序列化
object BaseComputing extends Serializable{
  //转化为LabelPoint dly 123 4 5
  def toLablePoint(data:RDD[(String,Array[Double])],label:Set[String]):RDD[(String,LabeledPoint)] = {
    data.map(line => {
      var s = new LabeledPoint(0.0,Vectors.dense(line._2))
      if (label.contains(line._1)) s = new LabeledPoint(1.0,Vectors.dense(line._2))
      (line._1,s)
    })
  }

  def getSelectFeatureData(data:RDD[(String,LabeledPoint)],item:Set[String]) = {
    data.filter(line => item.contains(line._1.split("_")(1)))
  }
  
  def getItemSet(data:RDD[String]):Set[String]={
    data.map(_.split(",")(0)).collect().toSet
  }

  //逻辑回归预测,num为预测的规模
  def lrPredict(data:RDD[(String,LabeledPoint)],model:LogisticRegressionModel,num:Int):Array[(String,Double)] = {
    data.map{case (userItem,LabeledPoint(label,features)) => {
      val prediction = model.clearThreshold().predict(Vectors.dense(features.toArray.map(line => Math.log(line + 1)))) //做了log处理
      (prediction,(userItem,label))
    }}.top(num).map(_._2)
  }
  //计算F值
  def calFvalue(data:Array[(String,Double)],buyedNextDay:Set[String]):String = {
    val count = data.size
    val orgin = buyedNextDay.size
    val acc = data.filter(_._2 == 1.0).size
    val accuracy = acc.toDouble/count;
    val recall = acc.toDouble/orgin;
    "predict_num:"+count+" accuracy:"+accuracy+" recall:"+recall+ " F1:"+ 2 * (recall*accuracy)/(accuracy + recall)
  }

  def svmPredict(data:RDD[(String,LabeledPoint)],model:SVMModel,num:Int):Array[(String,Double)] = {
    data.map{case (userItem,LabeledPoint(label,features)) => {
      val prediction = model.clearThreshold().predict(Vectors.dense(features.toArray.map(line => Math.log(line + 1)))) //做了log处理
      (prediction,(userItem,label))
    }}.top(num).map(_._2)
  }

  def gbrtPredict(data:RDD[(String,LabeledPoint)],model:GradientBoostedTreesModel,num:Int):Array[(String,Double)] = {
    data.map{case (userItem,LabeledPoint(label,features)) => {
      val prediction = model.predict(features) //做了log处理
      (prediction,(userItem,label))
    }}.top(num).map(_._2)
  }

  def getBuyLabel(data:RDD[String],date:String):Set[String]={
    data.filter(_.split(",")(5).split(" ")(0).equals(date)).
      filter(_.split(",")(2).equals("4")).map(line => {
      line.split(",")(0) + "_" + line.split(",")(1)+"_"+line.split(",")(4)
    }).distinct().collect().toSet
  }

  def getUserItemData(data:RDD[String])= {
    data.map(line => (line.split(",")(0) + "_" + line.split(",")(1)+"_"+line.split(",")(4), line)).
      groupByKey().map(line => (line._1, line._2.toArray.map(new Record(_))sortBy(_.time)))
  }

  def getUserData(data:RDD[String])={
    data.map(line => (line.split(",")(0), line)).
      groupByKey().map(line => (line._1, line._2.toArray.map(new Record(_))sortBy(_.time)))

  }

  def getItemData(data:RDD[String])={
    data.map(line => (line.split(",")(1), line)).
      groupByKey().map(line => (line._1, line._2.toArray.map(new Record(_))sortBy(_.time)))
  }

  def getCategoryData(data:RDD[String])={
    data.map(line => (line.split(",")(4), line)).
      groupByKey().map(line => (line._1, line._2.toArray.map(new Record(_))sortBy(_.time)))
  }
  //特征join
  def join( userItem:RDD[(String,Array[Double])],
            item:RDD[(String,Array[Double])],
            user:RDD[(String,Array[Double])]):RDD[(String,Array[Double])]={
    //和物品进行join
    val useritemJoinItem = userItem.map(line => (line._1.split("_")(1),line)).join(item).map(line => {
      val v = line._2
      (v._1._1,v._1._2++v._2)
    })
    val userMap = user.collect().toMap
    useritemJoinItem.map(line => {
      val userid = line._1.split("_")(0)
      val result = line._2.toBuffer ++ userMap(userid)
      (line._1,result.toArray)
    })
  }
}
