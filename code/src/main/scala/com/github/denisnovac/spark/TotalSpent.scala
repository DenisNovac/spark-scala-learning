package com.github.denisnovac.spark

import org.apache.spark._
import org.apache.log4j._

/*
id item amount_spent
44,8602,37.19
35,5368,65.89
2,3391,40.64
47,6694,14.98
 */

// how much was spen by each unique customer id
object TotalSpent:

  // customer id, spent
  private def parse(line: String): (String, Float) = {
    val fields = line.split(",")

    (fields(0), fields(2).toFloat)
  }

  @main def totalSpentF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")
    val input = sc.textFile("data/customer-orders.csv")

    input
      .map(parse)
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach(println)
