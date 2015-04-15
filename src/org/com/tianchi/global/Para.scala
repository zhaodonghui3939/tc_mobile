package org.com.tianchi.data.global

object Para {
  val path_data_user: String = "/data/tianchi/tianchi_mobile_recommend_train_user.csv"
  val path_data_item: String = "/data/tianchi/tianchi_mobile_recommend_train_item.csv"

  val train_start_date: String = "2014-11-18 0"
  val train_end_date: String = "2014-12-16 24"
  val train_label_date = "2014-12-17"

  val test_start_date: String = "2014-11-19 0"
  val test_end_date: String = "2014-12-17 24"
  val test_label_date: String = "2014-12-18"

  val real_start_date: String = "2014-11-20 0"
  val real_end_date: String = "2014-12-18 24"

  val negative_sample_fraction: Double = 0.00375
  val result_number = 850

  val gbt_num_iteration: Int = 40
  val gbt_tree_max_depth: Int = 5

}
