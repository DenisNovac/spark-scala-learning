package com.github.denisnovac.spark.advanced

import org.apache.log4j.*
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.*
import scala.collection.mutable

import scala.io.{Codec, Source}

/** Where do we store names per id?
  *
  * Join throughout the cluster - heavy operation
  *
  * Store map in memory - there is not much of a movies in a world
  *
  * Broadcast variables - sharing this map across cluster (transfers only once)
  *
  * u data: user movie rating ts
  *
  * 196 242 3 881250949
  *
  * 186 302 3 891717742
  */
object PopularMoviesWithNamesDs:

  // it is possible to extract only one column but whole schema is required anyway
  private final case class Movie(
      user_id: Int,
      movie_id: Int,
      rating: Int,
      timestamp: Long
  )

  object Movie:
    val schema: StructType =
      new StructType()
        .add("user_id", IntegerType, nullable = false)
        .add("movie_id", IntegerType, nullable = false)
        .add("rating", IntegerType, nullable = false)
        .add("timestamp", LongType, nullable = false)

  private def loadMovieNames(): Map[Int, String] = {
    // u.item not UTF 8
    implicit val codec: Codec = Codec("ISO-8859-1")
    val lines = Source.fromFile("data/ml-100k/u.item")

    val movieNames: mutable.Map[Int, String] = mutable.Map()

    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    lines.close()

    movieNames.toMap
  }

  @main def popularMoviesWithNamesDsF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("MoviesWithNames")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import scala3encoders.given
    import spark.implicits.*

    // upload map variable to cluster
    val nameDict: Broadcast[Map[Int, String]] =
      spark.sparkContext.broadcast(loadMovieNames())

    // .value allows to extract broadcasted variable
    val lookupName: Int => String = (movie_id: Int) => nameDict.value(movie_id)

    val ds: Dataset[Movie] =
      spark.read
        // the file is not csv - it is separated by tabs
        // but it could still be used with this workaround
        .option("sep", "\t")
        .schema(Movie.schema)
        .csv("data/ml-100k/u.data")
        .as[Movie]

    val movieCounts = ds
      .groupBy("movie_id")
      .agg(
        count("movie_id").alias("count"),
        round(avg("rating"), 2).alias("rating")
      )

    // spark udf is broken because of typetag too in Scala 3
    import scala3udf.{Udf => udf}
    // anonymous function could be udf
    // udf allows to pass column into anonymous function
    val lookupNameUdf = udf(lookupName)

    val withNames =
      movieCounts.withColumn("movie_title", lookupNameUdf(col("movie_id")))

    withNames
      .orderBy(desc("rating"))
      .show()

    spark.close()
