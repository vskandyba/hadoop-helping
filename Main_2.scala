import scala.collection.mutable

object Main_2 {

  def main(args: Array[String]): Unit = {

    // 1. Получил

    // 1. Чтение yml файла конфига из hdfs

    def uuid = java.util.UUID.randomUUID.toString

//    for(i <- 0 to 100){
//      println(uuid)
//    }



    println("Start " + Main.getCurrentTime())
    val parquetPath = "data/ini/inn/"
    val batches: mutable.Queue[String] = Test.getBathesQueue(parquetPath)(10000)
    println(Main.getCurrentTime())

    //batches.foreach(println)
    println("Finish: " +  Main.getCurrentTime())

/*
    val batches: mutable.Queue[String] = mutable.Queue[String]()

    batches.enqueue(""""1234124", "3223", "323242"""")
    batches.enqueue(""""3232354", "324", "6587"""")
    batches.enqueue(""""3452", "3223", "574"""")
    batches.enqueue(""""342", "123"""")

    //batches.foreach(println(_))

    println("before")
    batches.foreach(println(_))
    println("in progress")
    while(batches.nonEmpty){
     println(batches.dequeue())
    }
    println("after")
    batches.foreach(println(_))
*/



  }

}
