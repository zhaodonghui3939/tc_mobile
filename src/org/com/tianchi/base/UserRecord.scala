package org.com.tianchi.base

class UserRecord(val record:String) extends Serializable{
  //计算与18号0点的时间距离，精确到小时
  private def stringToInt(date: String): Int = {
    val date1 = date.split(" ")(0)
    (date1.split("-")(1).toInt - 11) * 30 * 24 + (date1.split("-")(2).toInt - 18) * 24 + date.split(" ")(1).toInt
  }
  override def toString()={
    record
  }

  //将每条记录解析出来
  val userId = record.split(",")(0)
  val itemId = record.split(",")(1)
  val behavior = record.split(",")(2)
  val geohash = record.split(",")(3)
  val category = record.split(",")(4)
  val time = stringToInt(record.split(",")(5))
}
