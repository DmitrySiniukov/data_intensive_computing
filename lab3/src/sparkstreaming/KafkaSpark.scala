package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    //val kafkaConf = Map[String, String]("metadata.broker.list" -> "localhost:9092")

    val conf = new SparkConf().setMaster("local[*]").setAppName("blabla")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val topicsSet = "avg".split(",").toSet

    //val messages = KafkaUtils.createDirectStream.<FILL IN>
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                      ssc, kafkaConf, topicsSet)
    val pairs = kafkaStream.map(msg => (msg._2)).map(x => (x.split(",")(0), x.split(",")(1).toDouble))
    
    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
        
      val avg = (value.getOrElse(0.0) + state.getOption.getOrElse(0.0)) / 2
      val output = (key, avg)
      state.update(avg)

      output
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.foreachRDD(entry => entry.saveToCassandra("avg_space", "words", SomeColumns("word", "count")))
    //stateDstream.saveToCassandra("test", "words", SomeColumns("word", "count")

    ssc.start()
    ssc.awaitTermination()
  }
}
