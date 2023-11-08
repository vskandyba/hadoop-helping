import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

object Test {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    //createSampleDataFrame.coalesce(1).write.mode(SaveMode.Overwrite).parquet("data/ini/test")

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.parquet("data/ini/test")
      .withColumn("row_number", row_number().over(Window.orderBy("VALUE")) - 1) // 0, 1, ...
      .cache()
    // df.where(col("row_number") === 0).show(false)
      
    val batchLimit = 7

    val map = splitDataFrame(df, batchLimit)

    map.keys.toList.sorted.foreach(x => {
      println(x)
      map(x).show(false)
    })
  }

  def splitDataFrame(df: DataFrame, batchLimit: Int): Map[String, DataFrame] ={

    val count: Int = df.count().toInt

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

  def createSampleDataFrame: DataFrame = {
    //s"key_$i", s"value_$i"
    val columns: Seq[String] = Seq("KEY", "VALUE")
    val values: Seq[(String, String)] = for (i <- 1 to 100) yield (s"key_$i", s"value_$i")

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val rdd = spark.sparkContext.parallelize(values)
    spark.createDataFrame(rdd).toDF(columns: _*)
  }
}
