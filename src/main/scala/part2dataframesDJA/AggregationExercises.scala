package part2dataframesDJA

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationExercises extends App {

  val spark = SparkSession.builder()
    .appName("Aggregation Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferScheme", "true")
    .json("src/main/resources/data/movies.json")

  // 1 Sum up all the profits of All the movies in the DF


  moviesDf
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))


// 2 count how many distinct directors we have

  moviesDf.select(countDistinct(col("Director"))).show()

// 3 Show the mean and standard deviation of US gross revenue for the movies
  moviesDf.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  // 4 Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
  moviesDf
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

}
