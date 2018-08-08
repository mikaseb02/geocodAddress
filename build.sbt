
name := "geocodAddress"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7",
  "org.apache.spark" %% "spark-sql-kafka-0-10_2.11" % "2.2.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


