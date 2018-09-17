package rocks.sblack.sparkstarter

import scala.util.parsing.json._
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.log4j.{Level, LogManager, Logger}
import java.text.SimpleDateFormat
import java.util.Date
import twitter4j.Status


object main {
  def main(args: Array[String]) {

    @transient lazy val log = LogManager.getLogger("myLogger")
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val configPath = "/tmp/config.json"
    val config = new config(configPath)

    val spark = SparkSession.builder().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val tweets = TwitterUtils.createStream(ssc, None)

    val db = new dbFactory(config.dbUser, config.dbPassword, config.dbConnString).get()
    db.run()

    def handleTweet(tweet: Status): String = {
      val id = tweet.getId
      val latlong: Option[String] = Option(tweet.getGeoLocation).map(l => s"${l.getLatitude},${l.getLongitude}")
      val place = Option(tweet.getPlace).map(_.getName)
      val text = tweet.getText
        .replace('\n', ' ')
        .replace('\r', ' ')
        .replace('\t', ' ')
      val backupDate = new Date(2000, 1, 1)
      val createdAtText = Option(tweet.getCreatedAt).getOrElse(backupDate)
      val createdAt = new SimpleDateFormat("dd/MMM/yyyy").format(createdAtText)
      val lang = Option(tweet.getLang).getOrElse("no")

      val insertQuery = "insert into " +
        "twitter.tweets (tweetId, createdAt, tweet, latlong, place, lang) " +
        s" values($id, '$createdAt', '$text', '$latlong', '$place', '$lang')"
      db.executeQuery(insertQuery)

      insertQuery
    }

    tweets.filter(!_.isPossiblySensitive)
      .map(handleTweet)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
