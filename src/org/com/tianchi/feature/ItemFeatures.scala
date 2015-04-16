package org.com.tianchi.data.feature

import org.apache.spark.rdd.RDD
import org.com.tianchi.base.Record
import scala.collection.mutable.ArrayBuffer

class ItemFeatures(data:RDD[(String,Array[Record])],begin:String,end:String) extends Serializable{
  private def stringToInt(date: String): Int = {
    val date1 = date.split(" ")(0)
    (date1.split("-")(1).toInt - 11) * 30 * 24 + (date1.split("-")(2).toInt - 18) * 24 + date.split(" ")(1).toInt
  }
  private val data_filtered:RDD[(String,Array[Record])] = data.map(line => {
    (line._1,line._2.filter(line => line.time < stringToInt(end) && line.time >= stringToInt(begin)
    ))}).filter(_._2.size > 0) //过滤数据

  private def getUserItemGPS={

  }

  private def calItemFeatures(featuresData:Array[Record])={
    //用户的总的点击购买收藏和购物车
    val click_sum = featuresData.size //行为总数目
    val buy_sum = featuresData.filter(_.behavior.equals("4")).size
    val favorite_sum = featuresData.filter(_.behavior.equals("2")).size
    val cart_sum = featuresData.filter(_.behavior.equals("3")).size
    val last_visit = featuresData(featuresData.size - 1).time - stringToInt(begin)
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
    val click_12h = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 12).size
    val click_24h = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 24).size
    val click_3d = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 3 * 24).size
    val click_5d = featuresData.filter(_.behavior.equals("1")).filter(_.time >= stringToInt(end) - 5 * 24).size

    //最近购买情况
    val buy_24h = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 24).size
    val buy_3d = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 3 * 24).size
    val buy_5d = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 5 * 24).size

    //最近收藏情况
    val favorite_24h = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 24).size
    val favorite_3d = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 3 * 24).size
    val favorite_5d = featuresData.filter(_.behavior.equals("2")).filter(_.time >= stringToInt(end) - 5 * 24).size

    //最近购物车情况
    val cart_24h = featuresData.filter(_.behavior.equals("3")).filter(_.time >= stringToInt(end) - 24).size

    val action_sum = featuresData.map(_.userId).distinct.size
    val action_6h = featuresData.filter(_.time >= stringToInt(end) - 6).map(_.userId).distinct.size
    val action_12h = featuresData.filter(_.time >= stringToInt(end) - 12).map(_.userId).distinct.size
    val action_24h = featuresData.filter(_.time >= stringToInt(end) - 24).map(_.userId).distinct.size
    val action_3d = featuresData.filter(_.time >= stringToInt(end) - 24 * 3).map(_.userId).distinct.size
    val action_5d = featuresData.filter(_.time >= stringToInt(end) - 24 * 5).map(_.userId).distinct.size

    val action_buy_sum = featuresData.filter(_.behavior.equals("4")).map(_.userId).distinct.size
    val action_buy_6h = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 6).map(_.userId).distinct.size
    val action_buy_12h = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 12).map(_.userId).distinct.size
    val action_buy_24h = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 24).map(_.userId).distinct.size
    val action_buy_3d = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 24 * 3).map(_.userId).distinct.size
    val action_buy_5d = featuresData.filter(_.behavior.equals("4")).filter(_.time >= stringToInt(end) - 24 * 5).map(_.userId).distinct.size

    val click_to_buy = buy_sum.toDouble / click_sum
    val favorite_to_buy = favorite_sum.toDouble / click_sum

    val action_to_buy_sum = action_buy_sum.toDouble / (action_sum + 1)
    val action_to_buy_6h = action_buy_6h.toDouble / (action_6h + 1)
    val action_to_buy_12h = action_buy_12h.toDouble / (action_12h + 1)
    val action_to_buy_24h = action_buy_24h.toDouble / (action_24h + 1)
    val action_to_buy_3d = action_buy_3d.toDouble / (action_3d + 1)
    val action_to_buy_5d = action_buy_5d.toDouble / (action_5d + 1)

    val features = ArrayBuffer[Double]()
    features += (click_sum,buy_sum,favorite_sum,cart_sum,
      last_visit,last_buy,last_favorite,last_cart,
      click_12h,click_24h,click_3d,click_5d,
      buy_24h,buy_3d,buy_5d,
      favorite_24h,favorite_3d,favorite_5d,cart_24h,
      action_sum,action_6h,action_12h,action_24h,action_3d,action_5d,
      click_to_buy,favorite_to_buy,
      action_sum,action_6h,action_12h,action_24h,action_3d,action_5d,
      action_buy_sum,action_buy_6h,action_buy_12h,action_buy_24h,action_buy_3d,action_buy_5d,
      action_to_buy_sum,action_to_buy_6h,action_to_buy_12h,action_to_buy_24h,action_to_buy_3d,action_to_buy_5d
      )
    features.toArray
  }

  def run():RDD[(String,Array[Double])] ={
    data_filtered.map( line => (line._1,calItemFeatures(line._2)))
  }
}
