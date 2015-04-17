package org.com.tianchi.feature

import org.apache.spark.rdd.RDD
import org.com.tianchi.base.{ItemRecord, UserRecord}
import scala.collection.mutable.ArrayBuffer

class UserItemGeohash(data: RDD[(String, Array[UserRecord])],
                      itemGeo: RDD[(String, Array[ItemRecord])], begin: String, end: String) extends Serializable{
  //根据开始和结束日期获得数据
  private def stringToInt(date: String): Int = {
    val date1 = date.split(" ")(0)
    (date1.split("-")(1).toInt - 11) * 30 * 24 + (date1.split("-")(2).toInt - 18) * 24 + date.split(" ")(1).toInt
  }

  private val data_filtered: RDD[(String, Array[UserRecord])] = data.map(line => {
    (line._1, line._2.filter(line => line.time < stringToInt(end) && line.time >= stringToInt(begin)
    ))
  }).filter(_._2.size > 0) //过滤数据

  //获取最活跃的位置
  private def getGeoHash(geos: Array[String]): String = {
    ""
  }

  private def getItemGeoHash(): RDD[(String, String)] = {
    itemGeo.map(line => (line._1, line._2.map(_.geoHash).filter(!_.equals("")))).map {
      case (item, records) => {
        if (records.length > 0) (item, records.reduce((a, b) => a + "," + b))
        else (item, "")
      }
    }
  }

  //计算最近时间的地理位置，目前以最近的位置，有很大的改进空间，比如计算用户的
  def getUserGeohash(): RDD[(String, String)] = {
    data_filtered.map {
      case (user_item_cata, records) => {
        val user = user_item_cata.split("_")(0)
        (user, records)
      }
    }.reduceByKey((a, b) => a ++ b).map {
      case (user, records) => {
        val t = records.sortBy(_.time).map(_.geohash).filter(!_.equals(""))
        if (t.length != 0) (user, t(t.length - 1))
        else (user, "")
      }
    }
  }

  //获取用户对商品的地理位置信息，以最近为主，其实也可以分析下行为情况
  def getUserItemGeoHash(): RDD[(String, String)] = {
    val userGeoHash = getUserGeohash().collect().toMap //将用户的地理位置转成Map
    data_filtered.map {
      case (user_item_cata, records) => {
        val user = user_item_cata.split("_")(0)
        val t = records.map(_.geohash).filter(!_.equals(""))
        if (t.length != 0) (user_item_cata, t(t.length - 1))
        else (user_item_cata, userGeoHash(user)) //如果没有距离，则以用户的为主
      }
    }
  }

  //  //类目地址猜测，中心点作为地点
  //  def getCatagory():RDD[(String,String)] = {
  //
  //  }
  //猜测商品的地址，以访问的用户的位置的中心点作为商品的地点位置,如果没有，则以类目的中心点作为距离，如何算中心，查阅geohash算法
  //  def getItemGeohash():RDD[(String,String)]={
  //    data_filtered.map{
  //      case (user_item_cata,records) => {
  //        val item = user_item_cata.split("_")(1)
  //        (item,records)
  //      }
  //    }.reduceByKey((a,b) => a ++ b).map{
  //      case (item,records) => {
  //        val t = records.sortBy(_.time).map(_.geohash).filter(!_.equals("")) //逻辑有误，应该是访问人群的中心距离
  //        if(t.length != 0) (item,t(t.length - 1))
  //        else (item,"")
  //      }
  //    }
  //  }
  private def dis(userGeo: String, itemGeo: String): Int = {
    if (userGeo.equals("") || itemGeo.equals("")) return 0
    val s1 = userGeo.toCharArray
    val s2 = itemGeo.toCharArray
    var count = 0
    for (i <- 0 to s1.length) {
      if (s1(i) == s2(i)) count = count + 1
      else return count
    }
    count
  }
  //计算用户对商品的距离
  def getUserItemGeoFeatures():RDD[(String,Int)] = {
    getUserItemGeoHash().map {
      case (userItem, geohash) => {
        (userItem.split("_")(1), (userItem, geohash))
      }
    }.leftOuterJoin(getItemGeoHash()).map {
      case (item, ((userItem, userGeo), Some(itemGeo))) => {
        if( itemGeo  == null)  (userItem, 0)
        else{
          val s = itemGeo.split(",")
          if(s.length > 1){
            val a = ArrayBuffer[Int]();
            for (c <- s) a += dis(userGeo,c)
            (userItem, a.min)
          }else (userItem, dis(userGeo,itemGeo))
        }
      }
    }
  }
}
