import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

Logger.getLogger("org").setLevel(Level.OFF)

val spark = SparkSession.builder().appName("csvFormatting")
  .enableHiveSupport()
  .master("local[*]").getOrCreate()

val sourceSchema: String = "foodmart"
val sourceTables: List[String] = spark.sql(s"show tables in $sourceSchema").collect.map(x => x(1).toString).toList

def write(sourceSchema: String, sourceTables: List[String])(writePath: String) = {
  import org.apache.spark.sql.SaveMode

  for(schemaName <- sourceTables){
    spark.sql(s"select * from $sourceSchema.$schemaName")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(writePath + schemaName)
  }
}

val writePath: String = "file:///root/parquets_foodmart/"
write(sourceSchema, sourceTables)(writePath)
