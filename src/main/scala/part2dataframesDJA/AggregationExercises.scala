package part2dataframesDJA

import org.apache.spark.sql.SparkSession

object AggregationExercises extends App {

  val spark = SparkSession.builder()
    .appName("Aggregation Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferScheme", "true")
    .json("src/main/resources/data/movies.json")

}
