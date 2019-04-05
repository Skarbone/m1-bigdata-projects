name := "Producer"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.12" % "2.1.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.11"


libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"