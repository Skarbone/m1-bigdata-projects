import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{SQLContext, SaveMode}

object Main extends App {






      val sparkConf = new SparkConf().setAppName("KafkaSparkCSV").setMaster("local[*]")
      val ssc = new StreamingContext(sparkConf, Seconds(600))
      val spark = SparkSession.builder().appName("Spark SQL").getOrCreate()



      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "kafkaCSV",
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
        if(!df.isEmpty){
          df.coalesce(1).write.mode(SaveMode.Append).option("header", "true").csv("/home/romain/Téléchargements/data.csv")
        }
      })
      ssc.start()
      ssc.awaitTermination()



}
