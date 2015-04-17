package org.com.tianchi.base

class ItemRecord(record:String) extends  Serializable{
  val itemId = record.split(",")(0)
  val geoHash = record.split(",")(1)
  val category = record.split(",")(2)
}
