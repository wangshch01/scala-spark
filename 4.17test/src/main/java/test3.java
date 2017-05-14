import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wangs on 2017/4/20.
 */
public class test3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("ssl2");
        //conf.setMaster("local");
        //创建java SparkContext sc
        JavaSparkContext sc = new JavaSparkContext(conf);
        //加载文本文件
        JavaRDD<String> rdd1 = sc.textFile(args[0]);
        //压扁
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] arr = s.split(" ");
                for(String ss: arr){
                    list.add(ss);
                }
                return list.iterator();
            }
        });

            //映射
            JavaPairRDD<String,Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s,1);
                }
            });

            //reduce化简
            JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });

            //
            List<Tuple2<String, Integer>> list = rdd4.collect();
            for (Tuple2<String, Integer> t : list) {
                System.out.println(t._1() + " : " + t._2());
            }


    }
}
