package part2dataframesDJA

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferScheme", "true")
    .json("src/main/resources/data/movies.json")


  // counting
  val generesCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting distinct

  moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count

  moviesDF.select(approx_count_distinct((col("Major_Genre")))) // Good for big data frames

  // min and max
  val minRatgingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")) // standard deviation In statistics, the standard deviation is a measure of the amount of variation or dispersion of a set of values. A low standard deviation indicates that the values tend to be close to the mean of the set, while a high standard deviation indicates that the values are spread out over a wider range.
  )

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // select count(*) from moviesDF group by Major_Genre  *** includes Null ***

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )

}
