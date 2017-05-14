import org.apache.spark.{SparkConf, SparkContext}

/** C:/Users/wangs/Desktop/4.20spark/he.log
  * Created by wangs on 2017/4/20.
  */
object test2 {

    def main(args: Array[String]): Unit = {
      new SparkContext(new SparkConf().setAppName("ssl").setMaster("local")).textFile("C:/Users/wangs/Desktop/4.20spark/he.log").flatMap(line => line.split("\\s")).filter (_.contains("Error")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println);

    }
}
