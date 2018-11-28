name := "PhoenixSparkSegy"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1.3.0.1.0-187" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1.3.0.1.0-187" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.1.3.0.1.0-187" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "3.1.1.3.0.1.0-187",
  "org.apache.hadoop" % "hadoop-aws" % "3.1.1.3.0.1.0-187",
  "org.apache.hbase" % "hbase-client" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-server" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-common" % "2.0.0.3.0.1.0-187",
  "org.apache.hbase" % "hbase-spark" % "2.0.0.3.0.1.0-187",
  "org.apache.flume" % "flume-ng-core" % "1.4.0.2.1.7.4-3",
  "org.apache.phoenix" % "phoenix-client" % "5.0.0.3.0.1.0-187"
)

assemblyJarName := "Yang.jar"

val meta = """META.INF(.)*""".r

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//assemblyMergeStrategy in assembly := {
//  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
//  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
//  case PathList("com", "google", xs @ _*) => MergeStrategy.last
//  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
//  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
//  case "about.html" => MergeStrategy.rename
//  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
//  case "META-INF/mailcap" => MergeStrategy.last
//  case "META-INF/mimetypes.default" => MergeStrategy.last
//  case "plugin.properties" => MergeStrategy.last
//  case "log4j.properties" => MergeStrategy.last
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}

logLevel := Level.Error
