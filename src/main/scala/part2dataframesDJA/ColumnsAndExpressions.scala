package part2dataframesDJA

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import part3typesdatasets.Datasets.guitarPlayerBandsDS.selectExpr

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

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn) //<- select method

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"), // <- column is more explicit
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  carsWithWeightsDF.show()

  // selectExpr

  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  //renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  //filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // Chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin"). distinct()
  allCountriesDF.show()


  println("-" * 50)

  // 1. Read the movies DF and select 2 columns of your choice

  val moviesDF = spark.read.option("inferSchems", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()

  val moviesReleaseDF2 = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )

}
