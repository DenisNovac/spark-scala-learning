package com.github.denisnovac.spark.sql

import org.apache.log4j.*
import org.apache.spark.sql.*

/*
id,name,age,friends
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
 */
object SparkSQLDataset:

  // dataset will inherit data schema from this class
  final case class Person(id: Int, name: String, age: Int, friends: Int)

  @main def sparkSQLDatasetF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use SparkSession interface instead of context
    val session = SparkSession.builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // no TypeTag in Scala 3, lib github.com/vincenzobaz/spark-scala3 helps
    // import session.implicits._
    import scala3encoders.given

    val peopleDs: Dataset[Person] =
      session.read
        .options {
          Map(
            "header" -> "true",
            "inferSchema" -> "true"
          )
        }
        // DataFrame = Dataset[Row], schema in runtime
        .csv("data/fakefriends.csv")
        .as[Person] // Dataset[Person]

    peopleDs.printSchema()
    /*
     root
     |-- id: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- age: integer (nullable = true)
     |-- friends: integer (nullable = true)
     */

    // basically the same as creating database table "people"
    // creates sql-accessible table "people" in session used to create this dataset
    peopleDs.createOrReplaceTempView("people")
    val teenagers: Dataset[Person] =
      session
        .sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
        .as[Person]

    // scala ddl with same result
    val teenagers2 =
      peopleDs
        .filter(p => p.age >= 13 && p.age <= 19)

    val results = teenagers.collect()

    val results2 = teenagers2.collect()

    results.foreach(println)
    /*
      [21,Miles,19,268]
      [52,Beverly,19,269]
      [54,Brunt,19,5]
     */

    println("================")

    results2.foreach(println)

    println()
    println(results.sameElements(results2)) // true

    // session should be closed at the end
    session.stop()
