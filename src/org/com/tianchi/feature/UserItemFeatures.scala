package org.com.tianchi.data.feature

import org.apache.spark.rdd.RDD
import org.com.tianchi.data.base.Record
import scala.collection.mutable.ArrayBuffer
class UserItemFeatures(data:RDD[(String,Array[Record])],begin:String,end:String) extends Serializable{
  //更具开始和结束日期获得数据
  private def stringToInt(date: String): Int = {
    val date1 = date.split(" ")(0)
    (date1.split("-")(1).toInt - 11) * 30 * 24 + (date1.split("-")(2).toInt - 18) * 24 + date.split(" ")(1).toInt
  }
  private val data_filtered:RDD[(String,Array[Record])] = data.map(line => {
    (line._1,line._2.filter(line => line.time < stringToInt(end) && line.time >= stringToInt(begin)
    ))}).filter(_._2.size > 0) //过滤数据

  private def calUserItemFeatures(featuresData:Array[Record])={
    //用户的总的点击购买收藏和购物车
    val click_sum = featuresData.size
    val buy_sum = featuresData.filter(_.behavior.equals("4")).size
    val favorite_sum = featuresData.filter(_.behavior.equals("2")).size
    val cart_sum = featuresData.filter(_.behavior.equals("3")).size

    val first_visit = featuresData(0).time - stringToInt(begin)
    val last_visit = featuresData(featuresData.size - 1).time - stringToInt(begin)

    val first_buy = {
      val s = featuresData.filter(_.behavior.equals("4"))
      if (s.size > 0) s(0).time - stringToInt(begin)
      else 0
    }

    val last_buy = {
      val s = featuresData.filter(_.behavior.equals("4"))
      if (s.size > 0) s(s.size - 1).time - stringToInt(begin)
      else 0
    }

    val last_favorite = {
      val s = featuresData.filter(_.behavior.equals("2"))
      if (s.size > 0) s(s.size - 1).time - stringToInt(begin)
      else 0
    }

    val last_cart = {
      val s = featuresData.filter(_.behavior.equals("3"))
      if (s.size > 0) s(s.size - 1).time - stringToInt(begin)
      else 0
    }

    //最近点击情况
    val click_6h = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 6).size
    val click_12h = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 12).size
    val click_18h = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 18).size
    val click_24h = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 24).size
    val click_3d = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 3 * 24).size
    val click_5d = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 5 * 24).size

    //最近购买情况
    val buy_24h = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 24).size
    val buy_3d = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 3 * 24).size
    val buy_5d = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 5 * 24).size

    //最近收藏情况
    val favorite_6h = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 6).size
    val favorite_12h = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 12).size
    val favorite_18h = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 18).size
    val favorite_24h = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 24).size
    val favorite_3d = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 3 * 24).size
    val favorite_5d = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 5 * 24).size

    //最近购物车情况
    val cart_6h = featuresData.filter(_.behavior.equals("3")).filter(_.time >= stringToInt(end) - 6).size
    val cart_12h = featuresData.filter(_.behavior.equals("3")).filter(_.time >= stringToInt(end) - 12).size
    val cart_18h = featuresData.filter(_.behavior.equals("3")).filter(_.time >= stringToInt(end) - 18).size
    val cart_24h = featuresData.filter(_.behavior.equals("3")).filter(_.time >= stringToInt(end) - 24).size

    val data = featuresData.map(_.time)
    val action_sum = data.distinct.size
    val action_6h = data.filter(_ >= stringToInt(end) - 6).distinct.size
    val action_12h = data.filter(_ >= stringToInt(end) - 12).distinct.size
    val action_24h = data.filter(_ >= stringToInt(end) - 24).distinct.size
    val action_3d = data.filter(_ >= stringToInt(end) - 24 * 3).distinct.size
    val action_5d = data.filter(_ >= stringToInt(end) - 24 * 5).distinct.size

    val click_to_buy = buy_sum.toDouble / click_sum;
    val favorite_to_buy = favorite_sum.toDouble / click_sum;

    val features = ArrayBuffer[Double]();
    features += (click_sum,buy_sum,favorite_sum,cart_sum,
      first_visit,last_visit,first_buy,last_buy,last_favorite,last_cart,
      click_6h,click_12h,click_18h,click_24h,click_3d,click_5d,
      buy_24h,buy_3d,buy_5d,
      favorite_6h,favorite_12h,favorite_18h,favorite_24h,favorite_3d,favorite_5d,
      cart_6h,cart_12h,cart_18h,cart_24h,
      action_sum,action_6h,action_12h,action_24h,action_3d,action_5d,
      click_to_buy,favorite_to_buy
      )
    features.toArray
  }

  def run():RDD[(String,Array[Double])] ={
    data_filtered.map( line => (line._1,calUserItemFeatures(line._2)))
  }
}
