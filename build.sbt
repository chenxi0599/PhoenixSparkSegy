name := "PhoenixSparkSegy"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1.3.0.1.0-187",
  "org.apache.spark" %% "spark-sql" % "2.3.1.3.0.1.0-187",
  "org.apache.hadoop" % "hadoop-common" % "3.1.1.3.0.1.0-187",
  "org.apache.hadoop" % "hadoop-aws" % "3.1.1.3.0.1.0-187",
  "org.apache.hbase" % "hbase-client" % "2.0.0.3.0.1.0-187",
  "org.apache.flume" % "flume-ng-core" % "1.4.0.2.1.7.4-3",
  "org.apache.phoenix" % "phoenix-client" % "5.0.0.3.0.1.0-187"
)
