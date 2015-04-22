import org.apache.spark.SparkContext

/**
 * Created by closure on 15/4/17.
 */
object Test {
  def main(args: Array[String]) {
    val sc = new SparkContext()


    val s = Array("a","a","b")
    sc.parallelize(s).saveAsTextFile("/data/sly")

    }
  }

}
