import scala.util.matching
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext



object Test {

   def main(args: Array[String]) {

      // From Spark 2.0.0 to onward use SparkSession (managing Context)val spark = SparkSession.builder.appName("twitter-sentiment-analysis").getOrCreate()      

      //val sc = spark.sparkContext
      //val conf = new SparkConf().setMaster("local[*]").setAppName("twitter-sentiment-analysis")
      val config = new SparkConf().setMaster("local[*]").setAppName("twitter-stream-sentiment")
      val sc = new SparkContext(config)       
      val ssc = new StreamingContext(sc, Seconds(30))

      //val ssc = new StreamingContext(spark, Seconds(30))
      //val sc = ssc.sparkContext

      sc.setLogLevel("ERROR")
      //import spark.implicits._

      // From Spark 2.0.0 to onward use SparkSession (managing Context)
      //val spark = SparkSession.builder.appName("twitter-sentiment-analysis").getOrCreate()
      //val sc = spark.sparkContext

      // Create Spark Streaming Context
      //val ssc = new StreamingContext(sc, Seconds(10))

      // Twitter App API Credentials - underlying twitter4j Library
      System.setProperty("twitter4j.oauth.consumerKey", "OfdkehLTNt5RPRco2rPuPTPZN")
      System.setProperty("twitter4j.oauth.consumerSecret", "KWSBuYlfPIJ42StMEW3Be6jNb6ohXn9kDm6cKkRK8g98RGCVif")
      System.setProperty("twitter4j.oauth.accessToken", "2655466249-BixeGG5lu1AXkc4TJMfGwmKv780FxdJiRM88iUM")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "Poyf01A8VCx3blxzv14RYyomltGMV03jxrpx1fRYGWqHP")

      val filters = Seq("$BTC", "bitcoin")
      val twitterStream = TwitterUtils.createStream(ssc, None, filters)
      val englishTweets = twitterStream.filter(_.getLang == "en")
      //englishTweets.map(_.getText).print()


      /*
      val dataDS = englishTweets.map { tweet =>
            //val sentiment = NLPManager.detectSentiment(tweet.getText)
            val sentiment = SentimentAnalysisUtils.detectSentiment(tweet.getText).toString
            //val tags = tweet.getHashtagEntities.map(_.getText.toLowerCase)
            //(tweet.getText, sentiment.toString, tags)
      }
      */
      
      val data = englishTweets.map { tweet =>
            val sentiment = NLPManager.detectSentiment(tweet.getText)
            (tweet.getText, sentiment.toString)
      }

      //val sqlContex = spark.sqlContext
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._
      //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      //    var dataRDD : org.apache.spark.rdd.RDD[(String,String,Array[String])] = sc.emptyRDD
      /*dataDS.cache().foreachRDD(rdd => {
      val df = sc.createDataFrame(rdd)
      df.show()
      df.createOrReplaceTempView("sentiments")
      sqlContex.sql("select * from sentiments limit 20").show()
      // Combine RDDs
      //      dataRDD.union(rdd)
      })
      */

      data.foreachRDD { rdd =>
            rdd.toDF().registerTempTable("sentiments")
            //rdd.show()
            sqlContext.sql("select * from sentiments limit 20").show()
      }
      

      ssc.start()
      ssc.awaitTermination()
   }
}


