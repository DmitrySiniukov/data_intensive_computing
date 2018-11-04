/*
  Based on work of Muhammad Furqan
*/

package Project

import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object NLPManager {

  val nlpProps: Properties = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  def detectSentiment(message: String): SENTIMENT_TYPE = {

    val pipeline = new StanfordCoreNLP(nlpProps)

    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length

    }

    val averageSentiment:Double = {
      if(sentiments.nonEmpty) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / sizes.sum

    if(sentiments.isEmpty) {
      weightedSentiment = 2.5
    }

    weightedSentiment match {
      case s if s <= 0.0 => NOT_UNDERSTOOD
      case s if s < 2.0 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 5.0 => POSITIVE
      case s if s >= 5.0 => NOT_UNDERSTOOD
    }

  }

  trait SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE

}
