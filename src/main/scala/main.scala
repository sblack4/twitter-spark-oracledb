package rocks.sblack.sparkstarter

import java.sql.{Connection, ResultSet}

import scala.util.parsing.json._
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.log4j.{Level, LogManager, Logger}
//import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date
import oracle.jdbc.pool.OracleDataSource
import twitter4j.Status


object main {
  def main(args: Array[String]) {
    @transient lazy val log = LogManager.getLogger("myLogger")

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val configPath = "/tmp/config.json"
    val fileContents = Source.fromFile(configPath).getLines.mkString
    val jsonConfig = JSON.parseFull(fileContents).get.asInstanceOf[Map[String, String]]
    val consumerKey = jsonConfig("consumerKey")
    val consumerSecret = jsonConfig("consumerSecret")
    val accessToken = jsonConfig("accessToken")
    val accessTokenSecret = jsonConfig("accessTokenSecret")

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val spark = SparkSession.builder().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val tweets = TwitterUtils.createStream(ssc, None)

    // TODO: filter for !isPossiblySensitive

    case class tweet(id: Long, createdAt: java.util.Date, tweet: String, latlong: String, place: String, lang: String)


//    def executeQuery(spark: SparkSession, command: String): Unit = {
//      val dbUser = "system"
//      val dbPassword = "oracle"
//      val dbDriverClass = "oracle.jdbc.pool.OracleDataSource"
//      val dbConnString = "jdbc:oracle:thin:@localhost:1521:xe"
//      val table = "twitter.tweets"
//
//      val dbConnProps = new Properties()
//      dbConnProps.put("user", dbUser)
//      dbConnProps.put("password", dbPassword)
//      dbConnProps.setProperty("Driver", dbDriverClass)
//
//      spark.sql(command)
//        .write
//        .mode(SaveMode.Append)
//        .jdbc(dbConnString, table, dbConnProps)
//
//    }

    def executeQuery(command: String): Unit = {
      @transient lazy val log = LogManager.getLogger("myLogger")
      val dbUser = "system"
      val dbPassword = "oracle"
      val dbConnString = "jdbc:oracle:thin:@localhost:1521:xe"
      val ods = new OracleDataSource()
      ods.setURL(dbConnString)
      ods.setUser(dbUser)
      ods.setPassword(dbPassword)
      val conn: Connection = ods.getConnection()
      val stmt = conn.createStatement()
      log.info(s"query: $command")
      try {
        val result: ResultSet = stmt.executeQuery(command)
        while (result.next())
          log.info(result.getString(1))
      } catch {
        // gives  SQLException when no return
        case e: java.sql.SQLException => log.error(e)
        // gives this when try to drop nonexistent user
        case e: java.sql.SQLSyntaxErrorException => log.error(e)
      }
      stmt.close()
    }

    val dropUser = "drop user twitter cascade"

    val dropTablespace = "drop tablespace twitter" +
      " INCLUDING CONTENTS " +
      "   AND DATAFILES "

    val makeTableSpaceCommand = "create tablespace twitter " +
      " DATAFILE 'tbs_twitter.dbf' " +
      " SIZE 40M ONLINE"

    val makeUserCommand = "CREATE USER twitter" +
      " IDENTIFIED BY oracle" +
      " DEFAULT TABLESPACE twitter" +
      " QUOTA UNLIMITED ON twitter" +
      " TEMPORARY TABLESPACE temp"

    val makeTableCommand = "create table twitter.tweets ( " +
      " tweetId NUMBER(20), " +
      " createdAt Date, " +
      " tweet VARCHAR2(300), " +
      " latlong VARCHAR2(60), " +
      " place VARCHAR2(60), " +
      " lang VARCHAR2(60) " +
      " ) " +
      " tablespace twitter "

    executeQuery(dropUser)
    executeQuery(dropTablespace)
    executeQuery(makeTableSpaceCommand)
    executeQuery(makeUserCommand)
    executeQuery(makeTableCommand)

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
      executeQuery(insertQuery)

      insertQuery
    }

    tweets.map(handleTweet).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
