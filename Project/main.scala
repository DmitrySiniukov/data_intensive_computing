import scala.util.matching
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf

object Test {

   def main(args: Array[String]) {

      // From Spark 2.0.0 to onward use SparkSession (managing Context)val spark = SparkSession.builder.appName("twitter-sentiment-analysis").getOrCreate()
      //val spark = SparkSession.builder.setMaster("local").appName("TwitterSentimentAnalysis").getOrCreate()
      //val sc = spark.sparkContext
      val conf = new SparkConf().setMaster("local[*]").setAppName("twitter-sentiment-analysis")
      val ssc = new StreamingContext(conf, Seconds(10))

      val sc = ssc.sparkContext
      sc.setLogLevel("ERROR")
      //import spark.implicits._

      // From Spark 2.0.0 to onward use SparkSession (managing Context)
      //val spark = SparkSession.builder.appName("twitter-sentiment-analysis").getOrCreate()
      //val sc = spark.sparkContext

      // Create Spark Streaming Context
      //val ssc = new StreamingContext(sc, Seconds(10))

      // Twitter App API Credentials - underlying twitter4j Library
      System.setProperty("twitter4j.oauth.consumerKey", "blabla")
      System.setProperty("twitter4j.oauth.consumerSecret", "blabla")
      System.setProperty("twitter4j.oauth.accessToken", "blabla")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "blabla")

      val filters = Seq("$BTC")
      val twitterStream = TwitterUtils.createStream(ssc, None, filters)
      val englishTweets = twitterStream.filter(_.getLang == "en")
      englishTweets.map(_.getText).print()

      ssc.start()
      ssc.awaitTermination()
   }
}


