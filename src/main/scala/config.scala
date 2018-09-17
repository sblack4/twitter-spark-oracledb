package rocks.sblack.sparkstarter

import scala.util.parsing.json._
import scala.io.Source
import org.apache.log4j.{Level, LogManager, Logger}


class config(filePath: String) {
  @transient lazy val log: Logger = LogManager.getLogger("myLogger")
  var fileContents: String = ""
  var jsonConfig: Map[String, String] = Map("" -> "")
  var consumerKey: String = ""
  var consumerSecret: String = ""
  var accessToken: String = ""
  var accessTokenSecret: String = ""
  var dbUser: String     = ""
  var dbPassword: String = ""
  var dbConnString: String = ""

  if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
    Logger.getRootLogger.setLevel(Level.WARN)
  }
  parse()

  def setupTwitter(jsonConfig: Map[String, String]): Unit = {
    consumerKey = jsonConfig("consumerKey")
    consumerSecret = jsonConfig("consumerSecret")
    accessToken = jsonConfig("accessToken")
    accessTokenSecret = jsonConfig("accessTokenSecret")
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  }

  def setDbInfo(jsonConfig: Map[String, String]): Unit = {
    dbUser = jsonConfig("dbUser")
    dbPassword = jsonConfig("dbPassword")
    dbConnString = jsonConfig("dbConnString")
  }

  def parse(): Unit = {
    try {
      fileContents = Source.fromFile(filePath).getLines.mkString
      jsonConfig = JSON.parseFull(fileContents).get.asInstanceOf[Map[String, String]]
      setupTwitter(jsonConfig)
      setDbInfo(jsonConfig)

    } catch {
      case e: Exception => {
        log.error("Error parseing config file \n ")
        e.printStackTrace()
      }
    }
  }
}
