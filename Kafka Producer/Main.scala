object Main extends App {
  import play.api.libs.json._
  import scala.io.Source
  import java.util.Properties
  import org.apache.kafka.clients.producer._
  def sendOneLine(myMessage: JsValue): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val topic="kafka-spark"
    val record = new ProducerRecord(topic,"key", myMessage.toString())

    producer.send(record)
    producer.close()
  }

  def sendData(line: Int = 0, file: JsValue): Unit = {
    if(line < file.as[JsArray].value.size){
      sendOneLine(file{line})
      sendData(line+1, file)
    }
  }

  val stream = "/home/romain/IdeaProjects/Producer/file.json"
  for(i <- Source.fromFile(stream).getLines()){
    sendData(0,Json.parse(i))
  }




}
