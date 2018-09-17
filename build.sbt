name := "spark-starter"
organization := "com.github.sblack4"
version := "0.01"
scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
//  "com.oracle.jdbc" % "ojdbc8" % "12.2.0.1",
  "log4j" % "log4j" % "1.2.17"
)