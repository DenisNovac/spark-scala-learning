package com.github.denisnovac.spark

import org.apache.spark.*
import org.apache.log4j.*
import org.apache.spark.rdd.RDD

/** Count amount of lines in a file through Spark job
  *
  * Should be 100.000 lines
  */
object HelloWorld:

  @main def helloWorldF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    // context is responsible for RDD management
    val sc = new SparkContext("local[*]", "HelloWorld")
    // distributed strings
    val lines: RDD[String] = sc.textFile("data/ml-100k/u.data")
    // action - returns local Long
    val numLines: Long = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

    // do distributed squaring on spark
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
    val squares: RDD[Int] = rdd1.map(x => x * x)
    println(squares.toLocalIterator.toList.mkString(", "))

    sc.stop()
