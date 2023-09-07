package com.github.denisnovac.spark

import org.apache.spark.*
import org.apache.log4j.*
import org.apache.spark.rdd.RDD

object FriendsByAge:

  // line format is ID, Name, Age, Friends
  private def parseLine(line: String): (Int, Int) =
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)

  @main def friendsByAgeF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "FriendsByAge")
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    // (age, numFriends)
    val rdd: RDD[(Int, Int)] = lines.map(parseLine)

    // (age, (totalFriends, totalInstances))
    val totalsByAge: RDD[(Int, (Int, Int))] =
      rdd
        // convert each numFriends value to a tuple of (numFriends, 1)
        .mapValues(x => (x, 1)) //  RDD[(Int, (Int, Int))]
        // sum up the total numFriends and total instances for each age
        .reduceByKey { case ((num1, countX), (num2, countY)) =>
          (num1 + num2, countX + countY)
        }

    // to compute the average we divide totalFriends / totalInstances for each age
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results: Array[(Int, Int)] =
      averagesByAge
        // sorting on cluster, ascending = false
        .sortBy(_._2, false)
        // collect should be used if array is small - data will be loaded into driver memory
        .collect()

    // sorted by amount of friends DESC
    results.foreach(println)
