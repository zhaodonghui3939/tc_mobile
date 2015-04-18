package org.com.tianchi.feature

import org.apache.spark.rdd.RDD
import org.com.tianchi.base.{ItemRecord, UserRecord}

import scala.collection.mutable.ArrayBuffer

class UserItemGeo(user_item_data: RDD[(String, Array[UserRecord])],
                  item_data: RDD[(String, Array[ItemRecord])], begin: String, end: String) extends Serializable {
  private def stringToInt(date: String): Int = {
    val date1 = date.split(" ")(0)
    (date1.split("-")(1).toInt - 11) * 30 * 24 + (date1.split("-")(2).toInt - 18) * 24 + date.split(" ")(1).toInt
  }

  private val user_item_cate_data_filtered: RDD[(String, Array[UserRecord])] = user_item_data.map(line => {
    (line._1, line._2.filter(line => line.time < stringToInt(end) && line.time >= stringToInt(begin)
    ))
  }).filter(_._2.length > 0) //过滤数据

  private val userData = user_item_cate_data_filtered.map { case (user_item_cate, records) =>
    val user_id = user_item_cate.split("_")(0)
    (user_id, records.map(line => (line.time, line.geoHash)))
  }.reduceByKey((a, b) => a ++ b).cache()

  private val itemData = user_item_cate_data_filtered.map { case (user_item_cate, records) =>
    val item_id = user_item_cate.split("_")(1)
    (item_id, records.map(line => (line.time, line.geoHash)))
  }.reduceByKey((a, b) => a ++ b).cache()

  val region_length = 5

  def toRegion(geoHash: String) = {
    if (geoHash == "")
      ""
    else
      geoHash.substring(0, region_length)
  }

  //noinspection ComparingUnrelatedTypes
  def getItemRegions: RDD[(String, String)] = {
    itemData.map(line => (line._1, line._2.map(p => toRegion(p._2)).distinct.filter(!_.equals("")))).map {
      case (item, records) =>
        if (records.length > 0) (item, records.reduce((a, b) => a + "," + b))
        else (item, "")
    }.filter(!_._2.equals(""))

    //    //对于未给出位置的商品，以发生在其上的行为的地点形成的区域中出现次数最多的作为该商品所在区域
    //    val item_all_guess_region = itemData.map {
    //      case (item_id, records) =>
    //        val tmp = records.map(line => toRegion(line._2)).filter(!_.equals(""))
    //        if (tmp.length == 0)
    //          (item_id, "")
    //        else {
    //          val value = tmp.groupBy(x => x).map {
    //            case (geoRegion, list) => (geoRegion, list.length)
    //          }.maxBy(_._2)._1
    //          (item_id, value)
    //        }
    //    }
    //
    //    item_all_guess_region.map {
    //      case (item_id, region) =>
    //        if (item_selected_real_region.contains(item_id))
    //          (item_id, item_selected_real_region(item_id))
    //        else
    //          (item_id, region)
    //    }

  }

  //求用户最近出现的地点
  def getUserRecentlyRegion(): RDD[(String, String)] = {
    userData.map {
      case (user_id, records) =>
        val tmp: Array[String] = records.sortBy(_._1).map(p => toRegion(p._2)).filter(!_.equals(""))
        if (tmp.length == 0)
          (user_id, "")
        else
          (user_id, tmp(tmp.length - 1))
    }
  }

  //求用户最常出现的区域
  def getUserMostlyRegion() = {
    userData.map {
      case (user_id, records) =>
        val tmp = records.map(line => toRegion(line._2)).filter(!_.equals(""))
        if (tmp.length == 0)
          (user_id, "")
        else {
          val value = tmp.groupBy(x => x).map {
            case (geoRegion, list) => (geoRegion, list.length)
          }.maxBy(_._2)._1
          (user_id, value)
        }
    }
  }

  def calRegionSimilarity(region1: String, region2: String): Int = {
    if (region1.equals("") || region2.equals(""))
      return 0
    val s1 = region1.toCharArray
    val s2 = region2.toCharArray
    var count = 0
    for (i <- 0 until s1.length) {
      if (s1(i) == s2(i)) count = count + 1
      else return count
    }
    count
  }

  def getUserItemDistance(f: () => RDD[(String, String)]) = {
    val userRegion = f()
    user_item_cate_data_filtered.map {
      case (user_item_cate, records) =>
        val user_id = user_item_cate.split("_")(0)
        val item_id = user_item_cate.split("_")(1)
        (user_id, item_id)
    }.join(userRegion).map {
      case (user_id, (item_id, userregion)) =>
        (item_id, (user_id, userregion))
    }.leftOuterJoin(getItemRegions).map {
      case (item_id, ((user_id, userregion), option_item_region)) =>
        option_item_region match {
          case Some(item_region) =>
            val similarity = item_region.split(",").map(calRegionSimilarity(_, userregion)).max
            (user_id + "_" + item_id, similarity)
          case None =>
            (user_id + "_" + item_id, 0)
        }
    }
  }

  //用户最近出现的地点到商品的距离
  def getUserRecentlyItemDistance: RDD[(String, Int)] = {
    getUserItemDistance(getUserRecentlyRegion)
  }

  //用户最常出现的地点到商品的距离
  def getUserMostlyItemDistance = {
    getUserItemDistance(getUserMostlyRegion)
  }

  //用户对该商品的行为中最近一次地点到商品的距离
  def getUserItemBehaveDistance = {
    user_item_cate_data_filtered.map { case (user_item_cate, records) =>
      val user_id = user_item_cate.split("_")(0)
      val item_id = user_item_cate.split("_")(1)
      val tmp = records.sortBy(_.time).map(p => toRegion(p.geoHash)).filter(!_.equals(""))
      if (tmp.length == 0)
        (user_id, item_id, "")
      else
        (user_id, item_id, tmp(tmp.length - 1))
    }.map {
      case (user_id, item_id, region) =>
        (item_id, (user_id, region))
    }.leftOuterJoin(getItemRegions).map {
      case (item_id, ((user_id, userregion), option_item_region)) =>
        option_item_region match {
          case Some(item_region) =>
            val similarity = item_region.split(",").map(calRegionSimilarity(_, userregion)).max
            (user_id + "_" + item_id, similarity)
          case None =>
            (user_id + "_" + item_id, 0)
        }
    }
  }

  //这是要调用的函数，会把三个距离组成一个Array，输出为(user_id_item_id,Array(dis1,dis2,dis3))
  def createUserItemGeoFeatures() = {
    getUserRecentlyItemDistance.join(getUserMostlyItemDistance).map {
      case (id, (dis1, dis2)) =>
        val a = ArrayBuffer[Int]()
        (id, (a += dis1 += dis2).toArray)
    }.join(getUserItemBehaveDistance).map {
      case (id, (dis, new_dis)) =>
        (id, (dis :+ new_dis).map(_.toDouble))
    }
  }
}
