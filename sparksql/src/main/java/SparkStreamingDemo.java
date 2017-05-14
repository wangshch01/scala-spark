import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by wangs on 2017/4/27.
 */
public class SparkStreamingDemo {
    public static void main(String[] args) throws InterruptedException {
       SparkConf conf = new SparkConf().setAppName("nc");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

       JavaReceiverInputDStream<String> lines = ssc.socketTextStream("s201",9999);

       JavaDStream<String> words = lines.flatMap(
               new FlatMapFunction<String, String>() {
                   public Iterator<String> call(String s) throws Exception {
                       return Arrays.asList(s.split(" ")).iterator();
                   }
               }
       );

       JavaPairDStream<String,Integer> pairs = words.mapToPair(
               new PairFunction<String, String, Integer>() {
                   public Tuple2<String, Integer> call(String s) throws Exception {
                       return new Tuple2<String, Integer>(s,1);
                   }
               }
       );

       JavaPairDStream<String,Integer> wordCounts = pairs.reduceByKey(
               new Function2<Integer, Integer, Integer>() {
                   public Integer call(Integer i1, Integer i2) throws Exception {
                       return i1 + i2;
                   }
               }
       );

       wordCounts.print();

       ssc.start();
       ssc.awaitTermination();
    }
}
