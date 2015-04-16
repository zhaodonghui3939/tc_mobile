package org.com.tianchi.feature

import org.apache.spark.rdd.RDD
import org.com.tianchi.base.UserRecord
class CataFeatures(data:RDD[(String,UserRecord)]) extends Serializable{

}
