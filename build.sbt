name := "SparkMachineLearning"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5"
libraryDependencies += "com.danielasfregola" %% "random-data-generator" % "2.1"
libraryDependencies += "net.debasishg" %% "redisclient" % "3.4"