import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangs on 2017/5/10.
 */

public class test1{

    @Test
    public void test1() {
        String caller = "13341109505";
        String year = "2017";
        SparkSession sess = SparkSession.builder().enableHiveSupport().appName("SparkHive").master("spark://s201:7077").getOrCreate();
        String sql = "select count(*) ,substr(calltime,1,6) from ext_calllogs_in_hbase " +
                "where caller = '" + caller + "' and substr(calltime,1,4) == '" + year
                + "' group by substr(calltime,1,6) order by substr(calltime,1,6)";
        Dataset<Row> df = sess.sql(sql);
        List<Row> rows = df.collectAsList();
        List<CallLogStat> list = new ArrayList<CallLogStat>();
        for (Row row : rows) {
            System.out.println(row.getString(1));
            list.add(new CallLogStat(row.getString(1), (int) row.getLong(0)));
        }
    }

}

