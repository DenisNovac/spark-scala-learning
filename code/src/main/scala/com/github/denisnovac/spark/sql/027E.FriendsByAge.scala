package com.github.denisnovac.spark.sql

import org.apache.log4j.*
import org.apache.spark.sql.*

/*
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21

average number of friends by age
 */
object FriendsByAgeSql:

  @main def friendsByAgeSqlF(): Unit =

    final case class Person(id: Int, name: String, age: Int, friends: Int)

    Logger.getLogger("org").setLevel(Level.ERROR)
    // Use SparkSession interface instead of context
    val session = SparkSession.builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._
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

    peopleDs.createOrReplaceTempView("people")

    // direct approach
    session
      .sql(
        """
          |SELECT age, CAST(avg(friends) AS INT) as avg
          |FROM people
          |GROUP BY age
          |ORDER BY avg DESC
          |""".stripMargin
      )
      .show()

    // ddl approach
    peopleDs
      .select("age", "friends")
      // groupBy makes type RelationalGroupedDataset which allows to call agg functions
      .groupBy("age")
      // avg is an aggregate function, we can call it only after the group by
      .avg("friends")
      .orderBy($"avg(friends)".desc)
      .show

    // .agg approach with custom column name

    // lot of utility functions including math functions such as rounding
    import org.apache.spark.sql.functions._

    peopleDs
      .select("age", "friends")
      .groupBy("age")
      .agg(
        round(avg("friends"), scale = 0)
          .alias("friends_avg")
      )
      .sort(desc("friends_avg"))
      .show
