name := "Spark-Kafka"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.12" % "2.1.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

libraryDependencies += "org.apache.commons" % "commons-email" % "1.5"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.11"
val sparkVersion = "2.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion,
  "org.apache.spark" % "spark-core_2.12" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion
  
)