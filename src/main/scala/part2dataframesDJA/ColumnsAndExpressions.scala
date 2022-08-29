package part2dataframesDJA

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods
  import spark.implicits
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs")
  )

}
