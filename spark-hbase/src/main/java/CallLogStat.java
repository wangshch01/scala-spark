/**
 * Created by wangs on 2017/5/10.
 */
public class CallLogStat {
    private int Count;
    private String YearMonth;

    public CallLogStat(String yearMonth, int count) {
        Count = count;
        YearMonth = yearMonth;
    }

    public CallLogStat() {
    }


    public int getCount() {
        return Count;
    }

    public void setCount(int count) {
        Count = count;
    }

    public String getYearMonth() {
        return YearMonth;
    }

    public void setYearMonth(String yearMonth) {
        YearMonth = yearMonth;
    }
}
