package com.github.denisnovac.spark.sql

import org.apache.log4j.*
import org.apache.spark.sql.*

object DataFramesDataset:

  final case class Person(id: Int, name: String, age: Int, friends: Int)

  @main def dataFramesDatasetF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use SparkSession interface instead of context
    val session = SparkSession.builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import scala3encoders.given

    val peopleDs: Dataset[Person] =
      session.read
        .options {
          Map(
            "header" -> "true",
            "inferSchema" -> "true" // required before parsing to actual schema
          )
        }
        // DataFrame = Dataset[Row], schema in runtime
        .csv("data/fakefriends.csv")
        .as[Person] // Dataset[Person]

    peopleDs.printSchema()

    // using SQL through special DDL

    peopleDs.select("name").show()

    peopleDs.filter(peopleDs("age") > 18).show()

    peopleDs
      .groupBy("age")
      .count()
      // .orderBy(peopleDs("count").desc) don't work - count is not in schema here
      .show()

    // could be done through raw SQL:
    peopleDs.createOrReplaceTempView("people")
    session
      .sql("""
        |SELECT age, COUNT(*) as c FROM people
        |GROUP BY age
        |ORDER BY c DESC
        |""".stripMargin)
      .show()

    peopleDs.select(peopleDs("name"), peopleDs("age") + 10).show()

    // session should be closed at the end
    session.stop()
