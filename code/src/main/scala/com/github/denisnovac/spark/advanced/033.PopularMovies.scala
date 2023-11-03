package com.github.denisnovac.spark.advanced

import org.apache.log4j.*
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.*

/**
user movie rating ts
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
244	51	2	880606923
 */
object PopularMoviesDs:

  // it is possible to extract only one column but whole schema is required anyway
  private final case class Movie(movie_id: Int)

  object Movie:
    val schema: StructType =
      new StructType()
        .add("user_id", IntegerType, nullable = false)
        .add("movie_id", IntegerType, nullable = false)
        .add("rating", IntegerType, nullable = false)
        .add("timestamp", LongType, nullable = false)

  @main def popularMoviesDsF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("TotalSpent")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import scala3encoders.given
    import spark.implicits.*

    val ds: Dataset[Movie] =
      spark.read
        // the file is not csv - it is separated by tabs
        // but it could still be used with this workaround
        .option("sep", "\t")
        .schema(Movie.schema)
        .csv("data/ml-100k/u.data")
        .as[Movie]

    ds
      .groupBy("movie_id")
      .count()
      .alias("count")
      .orderBy(desc("count"))
      .show()
