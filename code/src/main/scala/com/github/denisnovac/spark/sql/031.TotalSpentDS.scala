package com.github.denisnovac.spark.sql

import org.apache.log4j.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

/*
customer_id item amount_spent
44,8602,37.19
35,5368,65.89
2,3391,40.64
47,6694,14.98
 */
/* Total amount of spent by customer */
object TotalSpentDS:

  private final case class Purchase(
      customer_id: Int,
      item_id: Int,
      price: Double
  )

  private object Purchase:
    val schema: StructType =
      new StructType()
        .add("customer_id", IntegerType, nullable = false)
        .add("item_id", IntegerType, nullable = false)
        .add("price", DoubleType, nullable = false)

  @main def totalSpentDsF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("TotalSpent")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import scala3encoders.given
    import spark.implicits.*

    val ds: Dataset[Purchase] =
      spark.read
        .schema(Purchase.schema)
        .csv("data/customer-orders.csv")
        .as[Purchase]

    val totalSpent =
      ds.select("customer_id", "price")
        .groupBy("customer_id")
        .agg(round(sum("price"), 2).alias("total_spent"))
        .sort($"total_spent".desc)

    totalSpent.explain()
    /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Sort [total_spent#19 DESC NULLS LAST], true, 0
       +- Exchange rangepartitioning(total_spent#19 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=18]
          +- HashAggregate(keys=[customer_id#0], functions=[sum(price#2)])
             +- Exchange hashpartitioning(customer_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=15]
                +- HashAggregate(keys=[customer_id#0], functions=[partial_sum(price#2)])
                   +- FileScan csv [customer_id#0,price#2] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/denisiablochkin/dev/home/spark/code/data/customer-orders.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<customer_id:int,price:double>
     */

    totalSpent.show(10)

    spark.close()
