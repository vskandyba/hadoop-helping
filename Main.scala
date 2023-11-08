
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Calendar

object Main {

  Logger.getLogger("org").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    spark.conf.getAll.foreach(println)

    val conf = spark.conf

    //val sdf: DataFrame = spark.readStream.format("rate").load
    //sdf.explain()

//    println(
//      spark.read.text("data/inn").count()
//    )
    /*
        import org.apache.spark.sql.types._
        val innSchema: StructType = StructType(Array(
          StructField("inn", StringType, nullable = true)
        )
        )
        println(
          spark.read
            .schema(innSchema)
            .text("data/inn")
            .distinct()
            .write
            .parquet("data/ini/inn")
        )
         */

    // spark.read.parquet("data/ini/inn").show(false)


    //val sdf: DataFrame = spark.readStream.format("rate").load()

    val checkPointLocation = s"data/stg/chk_pat/console"
    val needCleanCheckPointLocation = true
    if (needCleanCheckPointLocation){
      import scala.sys.process._
      s"rm -rf $checkPointLocation"!!
    }

    val currentTime: String = getCurrentTime()

    //val checkPointLocation = s"data/stg/chk_pat/$currentTime"

    val srcPathString: String = "data/ini/test"
    //createSampleDataFrame.coalesce(1).write.mode(SaveMode.Overwrite).parquet(srcPathString)


    //val sdf: DataFrame = spark.readStream.format("parquet").option("inferSchema", "true").load(srcPathString)
    val schema = spark.read.parquet(srcPathString).schema
    println(schema)
    val sdf: DataFrame = spark.readStream.format("parquet").schema(schema).load(srcPathString)
    //val sdf: DataFrame = spark.readStream.schema(schema).parquet(srcPathString)
    val consoleSink: DataStreamWriter[Row] = createConsoleSink(sdf, checkPointLocation)

    // запустили вывод в консоль
    val streamingQuery: StreamingQuery = consoleSink.start()

    streamingQuery.awaitTermination()

//    while (streamingQuery.isActive){
//
//    }


  }

  def createConsoleSink(df: DataFrame, checkPointLocation:String): DataStreamWriter[Row] = {
    df
      .writeStream
      .format("console")
      .option("checkpointLocation", checkPointLocation)
      //.trigger(ProcessingTime("3 seconds"))
      .trigger(Trigger.ProcessingTime("3 second"))
      //.trigger(Trigger.Once())
      .option("numRows", "10")
//      .foreachBatch((batchDF: DataFrame, batchId: Long) =>
//        println(getCurrentTime())
//      )
  }

  def createSampleDataFrame: DataFrame = {
    //s"key_$i", s"value_$i"
    val columns: Seq[String] = Seq("KEY", "VALUE")
    val values: Seq[(String, String)] = for (i <- 1 to 100) yield (s"key_$i", s"value_$i")

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val rdd = spark.sparkContext.parallelize(values)
    spark.createDataFrame(rdd).toDF(columns: _*)
  }

  def getCurrentTime(formatString: String = "yyyy-mm-dd HH:mm:ss.mmm"): String = {
    import java.text.SimpleDateFormat
    import java.util.Calendar

    val format = new SimpleDateFormat(formatString)
    format.format(Calendar.getInstance().getTime) // 2023-21-08 22:21:39.021
  }

}
