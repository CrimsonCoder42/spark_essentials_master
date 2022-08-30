package part2dataframesDJA

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper



object CandEChallenges extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  // show movies

  val moviesDF = spark.read.option("inferSchems", "true").json("src/main/resources/data/movies.json")
  //moviesDF.show()

  // 1. Read the movies DF and select 2 columns of your choice

  //val moviesReleaseDF = moviesDF.select("Title", "Release Date")

//  val moviesReleaseDF2 = moviesDF.select(
//    moviesDF.col("Title"),
//    col("Release_Date"),
//    $"Major_Genre",
//    expr("IMBD_Rating")
//  )


  // 2.Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales

  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross"
  )

  // 3. Select all COMEDY movies with IMDB rating above 6

  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comediesDF3.show()




}
