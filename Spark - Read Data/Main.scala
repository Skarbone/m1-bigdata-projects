
import org.apache.spark.sql.SparkSession

object Main extends App {


  val spark = SparkSession.builder
    .master("local")
    .appName("data-exploration")
    .getOrCreate()

  val users = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/awaang78/Bureau/Python_Pour_Scala/fileCSV.csv")

  users.printSchema
  users.show(5)


  val usersFiltered = users
    .filter("weather =='CLEAR' ")
    .select(
      col = "state" , "temp", "drone_id"
    )
    .groupBy("drone_id")

  usersFiltered


  println("Number of faulty in the northern hemisphere ")
  users.filter("posX >= 0 AND mode == 'OFF'")
    .select(
      col = "state" , "temp", "drone_id"
    )
    .groupBy("state").count().show()


  println("Number of faulty in the southern hemisphere ")
  users.filter("posX < 0 AND mode == 'OFF'")
    .select(
      col = "state" , "temp", "drone_id"
    )
    .groupBy("state").count().show()


  println("Total number of faulty in the earth ")
  users.filter("mode == 'OFF' ")
    .select(
      col = "state" , "temp", "drone_id"
    )
    .groupBy("state").count().show()


  println("Total number of faulty according to weather")
  users.filter("mode == 'OFF' ")
    .select(
      col = "state" , "temp", "drone_id", "weather"
    )
    .groupBy("weather").count().show()


  println("When dead")
  users.filter("mode == 'OFF' ")
    .select(
      col = "state" , "temp", "drone_id", "weather"
    ).show()



  println("Low battery")
  users.filter("battery < 20 ")
    .select(
      col = "drone_id" , "battery", "posX", "posY"
    ).show()

  println("Many people")
  users.filter("people > 800 ")
    .select(
      col = "drone_id" ,"people", "people_angry", "groupX", "groupY"
    ).show()

  println("People angry")
  users.filter("people_angry / people > 0.5 ")
    .select(
      col = "drone_id" ,"people", "people_angry", "groupX", "groupY"
    ).show()

  println("Temperature problem")
  users.filter("state =='TEMPPB' AND (posX != 0.0 AND posY != 0.0) ")
    .select(
      col = "drone_id" , "temp", "state",  "posX", "posY"
    ).show()


}