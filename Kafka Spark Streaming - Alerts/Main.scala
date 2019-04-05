import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.commons.mail._


object Main extends App {

      def sendMail(title: String, msg: String): Unit = {
        val email = new SimpleEmail()
        email.setHostName("smtp.googlemail.com")
        email.setSmtpPort(465)
        email.setAuthenticator(new DefaultAuthenticator("riot.fetch.drone.alert@gmail.com", "RLG251197"))
        email.setSSLOnConnect(true)
        email.setFrom("riot.fetch.drone.alert@gmail.com")
        email.setSubject(title)
        email.setMsg(msg)
        email.addTo("riot.fetch.drone.alert@gmail.com")
        email.send()
      }



      val sparkConf = new SparkConf().setAppName("KafkaSparkAlert").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf, Seconds(20))
      val spark = SparkSession.builder().appName("Spark SQL 2").getOrCreate()




      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "kafkaAlert",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val topics = Array("kafka-spark")
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )


      stream.map(record => {
        (record.key, record.value)
      })
      stream.foreachRDD(rddRaw => {
        val rdd = rddRaw.map(_.value.toString)
        val df = spark.read.json(rdd)
        if(!df.isEmpty) {
          val alertOff =
          df.filter("mode == 'OFF'")
            .select(
              col = "state", "temp", "drone_id", "weather"
            ).collect().map(_.toSeq).mkString("\n")
          if (!(alertOff == null || alertOff.isEmpty)){
            sendMail("ALERTE : DRONE OFF", alertOff)
          }
          val alertLowBattery =
          df.filter("battery < 20 ")
            .select( col = "drone_id" , "battery", "posX", "posY"
            ).collect().map(_.toSeq).mkString("\n")
          if (!(alertLowBattery == null || alertLowBattery.isEmpty)){
            sendMail("ALERTE : LOW BATTERY", alertLowBattery)
          }
          val alertHugeCrowd =
            df.filter("people > 800 ")
              .select( col = "drone_id" ,"people", "people_angry", "groupX", "groupY"
              ).collect().map(_.toSeq).mkString("\n")
          if (!(alertHugeCrowd == null || alertHugeCrowd.isEmpty)){
            sendMail("ALERTE : HUGE CROWD", alertHugeCrowd)
          }
          val alertAngryPeople =
            df.filter("people_angry / people > 0.5 ") .select( col = "drone_id" ,"people", "people_angry", "groupX", "groupY"
            ).collect().map(_.toSeq).mkString("\n")
          if (!(alertAngryPeople == null || alertAngryPeople.isEmpty)){
            sendMail("ALERTE : ANGRY PEOPLE", alertAngryPeople)
          }
          val alertTemperatureProblem =
            df.filter("state =='TEMPPB' AND (posX != 0.0 AND posY != 0.0) ") .select( col = "drone_id" , "temp", "state", "posX", "posY"
            ).collect().map(_.toSeq).mkString("\n")
          if (!(alertTemperatureProblem == null || alertTemperatureProblem.isEmpty)){
            sendMail("ALERTE : TEMPERATURE PROBLEM", alertTemperatureProblem)
          }

        }
      })
      ssc.start()
      ssc.awaitTermination()



}
