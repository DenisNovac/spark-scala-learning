package com.github.denisnovac.spark.rdd

import org.apache.log4j.*
import org.apache.spark.*
import org.apache.spark.rdd.RDD

object WordCount:

  @main def wordCountF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")
    val input: RDD[String] = sc.textFile("data/book.txt")

    // Split into words
    val words: RDD[String] =
      input
        .flatMap(x => x.split("\\W+"))
        .map(_.toLowerCase)

    // Count up the occurrences of each word
    val wordCountsSparkSort =
      words
        // .countByValue() // - returns everything in memory do in distributed manner:
        .map(x => (x, 1L))
        .reduceByKey(_ + _)
        // might not work by itself (?) because sorting will happen in different partitions
        // and take will merge them unsorted
        .sortBy(_._2, ascending = false)
        .take(10) // will load into memory
        .toList

    // sorting the whole thing in memory, no partitions problem
    val wordCountsMemSort =
      words
        .countByValue() // load into memory
        .toList
        .sortWith((x, y) => x._2 > y._2)
        .take(10)

    wordCountsSparkSort.foreach(println)

    println()

    wordCountsMemSort.foreach(println)

    println()

    println(wordCountsSparkSort == wordCountsMemSort)
