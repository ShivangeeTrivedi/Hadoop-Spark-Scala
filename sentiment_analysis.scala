import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

object perform_sentiment_analysis {
  val preprocess = {
    val nlppro = new Properties()
    nlppro.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    nlppro
  }

  def getSentiment(text: String): String = {

    // Create a pipeline with NLP properties
    val pipeline = new StanfordCoreNLP(preprocess)

    // Run text through the Pipeline
    val annotation = pipeline.process(text)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longerone = 0
    var mSenti = 0

    for (tweetMsg <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val parseTree = tweetMsg.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val tweetSentiment = RNNCoreAnnotations.getPredictedClass(parseTree)
      val partText = tweetMsg.toString

      if (partText.length() > longerone) {
        mSenti = tweetSentiment
        longerone = partText.length()
      }

      sentiments += tweetSentiment.toDouble
      sizes += partText.length
    }

    val sentiment = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var final_sentiment = sentiment.sum / (sizes.fold(0)(_ + _))

    if (final_sentiment <= 2.0)
      "Neutral"
    else if (final_sentiment < 1.6)
      "NEGATIVE"
    else if (final_sentiment <= 0.0)
      "Not Applicable"
    else if (final_sentiment < 5.0)
      "POSITIVE"

    else "Not Applicable"
  }
}
