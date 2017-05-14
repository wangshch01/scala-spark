import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
import scala.util.Random._
/**
  * Created by wangs on 2017/4/21.
  */
object Test4 {

    def main(args: Array[String]): Unit = {
    val rDD =  new SparkContext(new SparkConf().setAppName("ssl")).textFile(args(0)).flatMap(line => line.split("\\s")).map((_, 1));
        val rdd2 = rDD.map(t =>{
            (t._1 +"_"+ Random.nextInt(100),1)
        })

    rdd2.reduceByKey(_ + _).map(t =>{
        (t._1.split("_")(0),t._2)
    }).reduceByKey(_ + _).collect().foreach(println)
    }
}
