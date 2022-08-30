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
  moviesDF.show()

  // 1. Read the movies DF and select 2 columns of your choice

  val moviesReleaseDF = moviesDF.select("Title", "Release Date")

//  val moviesReleaseDF2 = moviesDF.select(
//    moviesDF.col("Title"),
//    col("Release_Date"),
//    $"Major_Genre",
//    expr("IMBD_Rating")
//  )


  // 2.
}
