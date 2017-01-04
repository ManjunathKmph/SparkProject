name := "SparkProject"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.2"
libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.0.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"

crossPaths := false