import org.apache.spark.sql.*;

/**
 * Created by wangs on 2017/4/27.
 */
public class sql {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("SqlJava")
                .config("spark.master","local")
                .getOrCreate();
        Dataset<Row> df1 = session.read().json("file:///d:/scala/json.dat");

        //创建临时视图


    }
}
