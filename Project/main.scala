package Project

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
import org.elasticsearch.spark._
import java.net.URL
import play.api.libs.json._
import java.util.Calendar

object CryptoSentiment {

   def main(args: Array[String]) {

      // Initialize
      val config = new SparkConf().setMaster("local[*]").setAppName("twitter-crypto-sentiment")
      val sc = new SparkContext(config)       
      val ssc = new StreamingContext(sc, Seconds(30))

      // Avoid spark info and warning messaged
      sc.setLogLevel("ERROR")

      // Set Twitter API credentials
      System.setProperty("twitter4j.oauth.consumerKey", "blabla")
      System.setProperty("twitter4j.oauth.consumerSecret", "blabla")
      System.setProperty("twitter4j.oauth.accessToken", "blabla")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "blabla")

      // Filter tweets. We want bitcoin related, in english (for NLP)
      val filters = Seq("Bitcoin", "$BTC")
      val twitterStream = TwitterUtils.createStream(ssc, None, filters)
      val englishTweets = twitterStream.filter(_.getLang == "en")
      
      // PART 1 - For elasticsearch and Kibana
      
      englishTweets.foreachRDD{(rdd) =>

            // Get Bitcoin current market valuation
            val url = new URL("https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD")
            val result = scala.io.Source.fromURL(url).mkString
            val jsonObject = Json.parse(result)
            val price = (jsonObject \ "USD").as[Double]
            println("Bitcoin Price (USD): " + price)

            // Push to elasticsearch
            rdd.map(t => {
                  val (sentiment_int, sentiment) = SentimentAnalyzer.extractSentiment(t.getText)
                  Map(
                        "created_at" -> t.getCreatedAt.getTime.toString,
                        "text" -> t.getText,
                        "sentiment" -> sentiment,
                        "sentiment_int" -> sentiment_int,
                        "price" -> price
                  )
            }).saveToEs("sentiments/tweet")
      }

      // PART 2 - Alternative: For console vizualization

      val data = englishTweets.map { tweet =>
            val (sentiment_int, sentiment) = SentimentAnalyzer.extractSentiment(tweet.getText)
            (tweet.getText, sentiment.toString, sentiment_int)
      }

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      data.foreachRDD { rdd => 
            rdd.toDF().registerTempTable("sentiments")
            sqlContext.sql("select * from sentiments limit 15").show()
      }


      // Start streaming
      
      ssc.start()
      ssc.awaitTermination()
   }
}


