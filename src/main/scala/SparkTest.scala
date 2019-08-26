import scala.math.random
import org.apache.spark._

object SparkTest {
  def main(args:Array[String]){
    println(System.getenv("SPARK_LOCAL_IP"))
    println(args(0));
    val conf = new SparkConf().setAppName("SparkTest").setMaster("spark://47.103.19.138:17077").set("spark.executor.memory", "4096m")
      .setJars(List("D:\\idea\\SparkTest\\out\\artifacts\\SparkTest_jar\\SparkTest.jar")).setIfMissing("spark.driver.host", "bxhgrow.nat123.cc")
    val spark = new SparkContext(conf)
    val slices = if(args.length > 0) args(0).toInt else 2
    val n = 10 * slices
    val count = spark.parallelize(1 to n, slices).map{ i =>
      val x = random * 2 -1
      val y = random * 2 -1
      if(x * x + y *  y <1) 1 else 0
    }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n)
    spark.stop();
  }
}
