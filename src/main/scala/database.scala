package rocks.sblack.sparkstarter

import java.sql.{Connection, ResultSet}
import org.apache.log4j.LogManager
import oracle.jdbc.pool.OracleDataSource


trait database extends  Serializable{
  def executeQuery(command: String): Unit
  def run(): Unit
}

class dbFactory(dbUser: String="", dbPassword: String="", dbConnString: String="") {
  def get(): database = {
    if (! dbUser.isEmpty && ! dbPassword.isEmpty && ! dbConnString.isEmpty) {
      return new oracleDb(dbUser, dbPassword, dbConnString)
    }
    new hiveDb()
  }
}

class oracleDb(dbUser: String, dbPassword: String, dbConnString: String) extends database {
  val dropUser = "drop user twitter cascade"

  val dropTablespace: String = "drop tablespace twitter" +
    " INCLUDING CONTENTS " +
    "   AND DATAFILES "

  val makeTableSpaceCommand: String = "create tablespace twitter " +
    " DATAFILE 'tbs_twitter.dbf' " +
    " SIZE 40M ONLINE"

  val makeUserCommand: String = "CREATE USER twitter" +
    " IDENTIFIED BY oracle" +
    " DEFAULT TABLESPACE twitter" +
    " QUOTA UNLIMITED ON twitter" +
    " TEMPORARY TABLESPACE temp"

  val makeTableCommand: String = "create table twitter.tweets ( " +
    " tweetId NUMBER(20), " +
    " createdAt Date, " +
    " tweet VARCHAR2(300), " +
    " latlong VARCHAR2(60), " +
    " place VARCHAR2(60), " +
    " lang VARCHAR2(60) " +
    " ) " +
    " tablespace twitter "

  def executeQuery(command: String): Unit = {
    @transient lazy val log = LogManager.getLogger("dbLogger")

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


  def run(): Unit = {
    executeQuery(dropUser)
    executeQuery(dropTablespace)
    executeQuery(makeTableSpaceCommand)
    executeQuery(makeUserCommand)
    executeQuery(makeTableCommand)
  }
}

class hiveDb() extends database {
  val createTables = ""

  def executeQuery(command: String): Unit = {
    ???
  }

  def run(): Unit = {
    ???
  }
}