import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
  * Created by wangs on 2017/4/27.
  */
object SparkStreamingdemo {


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("nc")
        //创建spark流上下文，批处理时长1s
        val ssc = new StreamingContext(conf,Seconds(1));

        //创建scoket文本流
        val lines = ssc.socketTextStream("localhost",9999)
        //flatMap
        val words = lines.flatMap(_.split(" "))
        //变换成对偶
        val pairs = words.map((_,1))

        val count = pairs.reduceByKey(_+_);
        count.print()
        ssc.start()
        ssc.awaitTermination()

    }
}
