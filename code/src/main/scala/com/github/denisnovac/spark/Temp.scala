package com.github.denisnovac.spark

import org.apache.spark.*
import org.apache.log4j.*
import org.apache.spark.rdd.RDD

import scala.math.min
import scala.math.max

// temp is C multipled by 10
/*
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
 */

object Temp:

  private enum TempUnit:
    case C
    case F

  private enum Mode(val entry: String):
    case Min extends Mode("TMIN")
    case Max extends Mode("TMAX")

  private def parseLine(unit: TempUnit)(line: String): (String, String, Float) =
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)

    val cTemp = fields(3).toFloat * 0.1f

    unit match {
      case TempUnit.C =>
        (stationID, entryType, cTemp)

      case TempUnit.F =>
        val fTemp = cTemp * (9.0f / 5.0f) + 32.0f
        (stationID, entryType, fTemp)
    }

  private def parseRdd(
      mode: Mode
  )(rdd: RDD[(String, String, Float)]): RDD[(String, Float)] =
    rdd
      .filter(x => x._2 == mode.entry)
      // remove mode from tuples since we filtered by it
      .map(x => (x._1, x._3))
      .reduceByKey { (x, y) =>
        mode match
          case Mode.Min =>
            min(x, y)
          case Mode.Max =>
            max(x, y)
      }

  @main def tempF(unitArg: String, modeArg: String): Unit =

    // parse arguments such as F Min
    val (unit, mode) = {
      val u = TempUnit.values
        .find(t => t.toString.toLowerCase == unitArg.toLowerCase)
        .get

      val m =
        Mode.values
          .find(m => m.toString.toLowerCase == modeArg.toLowerCase)
          .get

      (u, m)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Temperatures")
    // two stations data for the 1800 year
    val lines = sc.textFile("data/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines: RDD[(String, String, Float)] =
      lines.map(parseLine(unit))

    val tempsByStation: RDD[(String, Float)] =
      parseRdd(mode)(parsedLines)

    // Collect, format, and print the results
    val results = tempsByStation.collect()

    // result is a minimum temperature for both of two stations
    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f $unit"
      println(s"$station $mode temperature: $formattedTemp")
    }
