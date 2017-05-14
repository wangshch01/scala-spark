import java.net.{InetAddress, Socket}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangs on 2017/5/7.
  */
object DeployModeTest {
    def printInfo(str:String):Unit = {

        val ip = InetAddress.getLocalHost.getHostAddress;
    val sock = new Socket("192.168.202.133",8888)
    val out = sock.getOutputStream;
    out.write((ip + ":" + str + "\r\n").getBytes())
    out.flush()
    sock.close()
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("DeployModeTest")
        val sc = new SparkContext(conf)
        printInfo("hello world")

        val rdd1 = sc.parallelize(1 to 10, 3)
        val rdd2 = rdd1.map(e => {
            printInfo(" map : " + e)
            e * 2;
        })
        val rdd3 = rdd2.repartition(2)
        val rdd4 = rdd3.map(e => {
            printInfo(" map2 : " + e)
            e
        })
        val res = rdd4.reduce((a, b) => {
            printInfo("reduce : " + a + "," + b)
            a + b;
        })
        printInfo("driver : " + res + "")





    }
}
