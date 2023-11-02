package com.github.denisnovac.spark.rdd

import org.apache.log4j.*
import org.apache.spark.*
import org.apache.spark.rdd.RDD

// Number of a given rating within movie set (100.000 movie ratings).
object RatingsCounter:

  @main def ratingsCounterF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines: RDD[String] = sc.textFile("data/ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings: RDD[String] = lines.map(x => x.split("\t")(2))

    // Count up how many times each value (rating) occurs

    /* This method should only be used if the resulting map is expected to be small, as the whole thing is loaded
     * into the driver's memory. To handle very large results, consider using
     * rdd.map(x => (x, 1L)).reduceByKey(_ + _) , which returns an RDD[T, Long] instead of a map.*/

    val results: scala.collection.Map[String, Long] = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults: Seq[(String, Long)] = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)
