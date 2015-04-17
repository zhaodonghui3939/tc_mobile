package org.com.tianchi.feature

import org.apache.spark.rdd.RDD
import org.com.tianchi.base.{ItemRecord, UserRecord}

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

  private val userData = user_item_cate_data_filtered.map { case (user_item_cate, records) => {
    val user_id = user_item_cate.split("_")(0)
    (user_id, records)
  }
  }.reduceByKey((a, b) => a ++ b).cache()

  private val itemData = user_item_cate_data_filtered.map { case (user_item_cate, records) => {
    val item_id = user_item_cate.split("_")(1)
    (item_id, records)
  }
  }.reduceByKey((a, b) => a ++ b).cache()

  val region_length = 5

  def toRegion(geoHash: String) = {
    geoHash.substring(0, region_length)
  }

  def getItemRegions(): RDD[(String, String)] = {
    val item_selected_real_region = item_data.map(line => (line._1, line._2.map(p => toRegion(p.geoHash)).distinct.filter(!_.equals("")))).map {
      case (item, records) => {
        if (records.length > 0) (item, records.reduce((a, b) => a + "," + b))
        else (item, "")
      }
    }.filter(!_._2.equals("")).collect().toMap

    //对于未给出位置的商品，以发生在其上的行为的地点形成的区域中出现次数最多的作为该商品所在区域
    val item_all_guess_region = itemData.map {
      case (item_id, records) => {
        val tmp = records.map(line => toRegion(line.geohash)).filter(!_.equals(""))
        if (tmp.length == 0)
          (item_id, "")
        else {
          val value = tmp.groupBy(x => x).map {
            case (geoRegion, list) => (geoRegion, list.length)
          }.maxBy(_._2)._1
          (item_id, value)
        }
      }
    }

    item_all_guess_region.map {
      case (item_id, region) => {
        if (item_selected_real_region.contains(item_id))
          (item_id, item_selected_real_region(item_id))
        else
          (item_id, region)
      }
    }

  }

  //求用户最近出现的地点
  def getUserRecentlyRegion = {
    userData.map {
      case (user_id, records) =>
        val tmp: Array[String] = records.sortBy(_.time).map(p => toRegion(p.geohash)).filter(!_.equals(""))
        if (tmp.length == 0)
          (user_id, "")
        else
          (user_id, tmp(tmp.length - 1))
    }
  }

  //求用户最常出现的区域
  def getUserMostlyRegion = {
    userData.map {
      case (user_id, records) => {
        val tmp = records.map(line => toRegion(line.geohash)).filter(!_.equals(""))
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
    return count
  }

  def getUserRecentlyItemDistance = {
    val userRegion = getUserRecentlyRegion
    user_item_cate_data_filtered.map {
      case (user_item_cate, records) => {
        val user_id = user_item_cate.split("_")(0)
        val item_id = user_item_cate.split("_")(1)
        (user_id, item_id)
      }
    }.join(userRegion).map {
      case (user_id, (item_id, userRecentRegion)) =>
        (item_id, (user_id, userRecentRegion))
    }.join(getItemRegions()).map {
      case (item_id, ((user_id, userRecentRegion), item_region)) =>
        (user_id + "_" + item_id, )

    }

  }

  def getUserMostlyItemDistance = {

  }

  def getUserNearlyItemDistance = {}

}
