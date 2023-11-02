package com.github.denisnovac.spark.sql

import org.apache.log4j.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{
  FloatType,
  IntegerType,
  StringType,
  StructType
}
import org.apache.spark.sql.{DataFrame, Dataset, SQLImplicits, SparkSession}

// temp is C multipled by 10
/*
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
 */
/** Find the minimum/maximum temperature by weather station */
object TempDS:

  private final case class Temperature(
      station_id: String,
      date: Int,
      measure_type: String,
      temperature: Float
  )

  private object Temperature:
    // we don't have header in csv - so we just name our column in schema
    val schema: StructType =
      new StructType()
        .add("station_id", StringType, nullable = false)
        .add("date", IntegerType, nullable = false)
        .add("measure_type", StringType, nullable = false)
        .add("temperature", FloatType, nullable = false)

  private enum Mode(val entry: String):
    case Min extends Mode("TMIN")
    case Max extends Mode("TMAX")

  private def getTemps(ds: Dataset[Temperature], mode: Mode)(
      implicits: SQLImplicits
  ): DataFrame = {
    import implicits.*

    val modeName = mode match
      case Mode.Min => "min_"
      case Mode.Max => "max_"

    // C is multiplied by 10 in file
    val withCorrectC =
      ds
        .select(
          $"station_id",
          round($"temperature" * 0.1, scale = 2).alias("temperature")
        )

    val filtered =
      withCorrectC
        .filter($"measure_type" === mode.entry)
        .groupBy($"station_id")

    val aggByMode = mode match
      case Mode.Min =>
        filtered.agg(min("temperature").alias("C"))
      case Mode.Max =>
        filtered.agg(max("temperature").alias("C"))

    val withF =
      aggByMode
        .withColumn(
          "F",
          round($"C" * (9.0f / 5.0f) + 32.0f, 2)
        )
        .sort($"F".asc)
        .withColumnRenamed("C", modeName + "C")
        .withColumnRenamed("F", modeName + "F")

    withF
  }

  @main def tempDsF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    import scala3encoders.given

    val temps: Dataset[Temperature] =
      spark.read
        .schema(Temperature.schema)
        .csv("data/1800.csv")
        .as[Temperature]

    val mins = getTemps(temps, Mode.Min)(spark.implicits)
    val maxes = getTemps(temps, Mode.Max)(spark.implicits)

    mins.show()

    maxes.show()

    spark.close()
