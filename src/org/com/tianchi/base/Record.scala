package org.com.tianchi.base

class Record(val record:String) extends Serializable{
  private def stringToInt(date: String): Int = {
    val date1 = date.split(" ")(0)
    (date1.split("-")(1).toInt - 11) * 30 * 24 + (date1.split("-")(2).toInt - 18) * 24 + date.split(" ")(1).toInt
  }
  override def toString()={
    record
  }
  val userId = record.split(",")(0);
  val itemId = record.split(",")(1);
  val behavior = record.split(",")(2);
  val geohash = record.split(",")(3);
  val category = record.split(",")(4);
  val time = stringToInt(record.split(",")(5))
}
