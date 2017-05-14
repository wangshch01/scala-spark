import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangs on 2017/5/3.
  */
object RecommDemo {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("rc").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = sc.textFile("file:///C:\\Users\\wangs\\Desktop\\5.03spark/data2.txt")
        //变换数据成为rating
        val ratings = data.map(_.split(",") match {
            case Array(user,item,rate) => Rating(user.toInt, item.toInt, rate.toDouble)
        })
        //Build the recommendation model using ALS
        val rank = 10
        val numIterations = 10
        //交替最小二乘法构建推荐模型
        val model = ALS.train(ratings,rank,numIterations,0.01)

        //取出评分数据的（user，product）
        val userProducts = ratings.map {case Rating(user,product,rate) => (user,product)}


        //通过model对(user,product)进行预测,((user, product),rate)
        val ug2 = sc.makeRDD(Array((2, 3), (2, 4)))
        val predictions =
            model.predict(ug2).map { case Rating(user, product, rate) =>
                ((user, product), rate)
            }
        predictions.collect().foreach(println)


    }

}
