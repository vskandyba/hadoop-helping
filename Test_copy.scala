import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object Test_copy {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    //createSampleDataFrame.coalesce(1).write.mode(SaveMode.Overwrite).parquet("data/ini/test")

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.parquet("data/ini/test")
      .withColumn("row_number", row_number().over(Window.orderBy("VALUE")) - 1) // 0, 1, ...
      .cache()

    val batchLimit = 7
    val map = splitDataFrame(df, batchLimit)

    val tmpPath = "data/stg/tmp_inn/"
    val timeSleep = 3 * 1000

    map.keys.toList.sortWith((x_1, x_2) => x_1.split("_").last.toInt < x_2.split("_").last.toInt)
      .foreach(x => {
      println(x)
      //val df = map(x).show(false)
      val df = map(x)
      //val bathPath = tmpPath + x; println(bathPath)
      df.coalesce(1).write.mode(SaveMode.Append).parquet(tmpPath)
      Thread.sleep(timeSleep)
      })
  }

  def splitDataFrame(df: DataFrame, batchLimit: Int): Map[String, DataFrame] ={

    val count: Int = df.count().toInt
    println("Count: " + Main.getCurrentTime())

    val bathCount = (count / batchLimit) + (if (count % batchLimit == 0) 0 else 1)
    println(bathCount)

    var batchesMap: Map[String, DataFrame] = Map.empty

    for (i <- 0 until bathCount) {
      val batchName: String = s"batch_$i"
      val batchDF: DataFrame = df.where(col("row_number").between(i * batchLimit, i * batchLimit + batchLimit))

      batchesMap += batchName -> batchDF
    }
    batchesMap
  }


  def getBathesQueue(parquetPath: String, columnName: String = "inn")(batchRowLimit: Int): mutable.Queue[String] ={
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    println("Before read parquet " + Main.getCurrentTime())
    val df: DataFrame = spark.read.parquet(parquetPath)
      .withColumn("row_number", row_number().over(Window.orderBy(columnName)) - 1) // 0, 1, ...
      .cache()
    println("After read parquet " + Main.getCurrentTime())

    val batchesMap = splitDataFrame(df, batchRowLimit)
    println("After read split " + Main.getCurrentTime())
    val batches: mutable.Queue[String] = mutable.Queue[String]()
    var i = 0
    batchesMap.values.foreach(
      valueDF => {
        println(s"$i")
        i = i + 1
        batches.enqueue(valueDF.select(col(columnName)).collect().map(raw => s"""\"${raw.mkString}\"""").mkString(","))
      }
    )
    df.unpersist()

    batches
  }


  def createSampleDataFrame: DataFrame = {
    //s"key_$i", s"value_$i"
    val columns: Seq[String] = Seq("KEY", "VALUE")
    val values: Seq[(String, String)] = for (i <- 1 to 100) yield (s"key_$i", s"value_$i")

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val rdd = spark.sparkContext.parallelize(values)
    spark.createDataFrame(rdd).toDF(columns: _*)
  }
}
