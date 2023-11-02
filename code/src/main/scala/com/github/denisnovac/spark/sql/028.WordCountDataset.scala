package com.github.denisnovac.spark.sql

import org.apache.log4j.*
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.*

/** Format just a txt file:
  *
  * Self-Employment: Building an Internet Business of One Achieving Financial
  * and Personal Freedom through a Lifestyle Technology Business By Frank Kane
  */

/** Count up how many of each word appears in a book as simply as possible. */
object WordCountDataset:

  // its not even a schema actually, Datasets work with structure data best but this is just lines
  // "value" is a default column when data unstructured and schema can't be inherited from data
  // to make it possible to cast to Line type - need to call field this way or write a schema manually
  private final case class Line(value: String)

  @main def wordCountDatasetF(): Unit =
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    import scala3encoders.given

    val input = spark.read.text("data/book.txt").as[Line]

    // explode() - similar to flatmap, explode columnst into rows
    // $"value" - column value done through $
    // =!=, === equality operators from spark

    val noExplode =
      input
        .select(split($"value", pattern = "\\W+").alias("line"))

    // noExplode.show()
    /** |                 line |
      * |---------------------:|
      * | [Self, Employment... |
      * | [Achieving, Finan... |
      * |    [By, Frank, Kane] |
      */

    val words: Dataset[String] =
      input
        // explode is basically a flatMap here when we do select of a DataSet[List[String]] and we want to work with inner things as with DataSet
        .select(explode(split($"value", pattern = "\\W+")).alias("word"))
        .filter($"word" =!= "")
        .as[String]

    // words.show()
    /** |       word |
      * |-----------:|
      * |       Self |
      * | Employment |
      * |   Building |
      */

    def wordsPipeline(words: Dataset[String]): DataFrame =
      // or can just do groupBy(lower(word))
      val lowerWords =
        words
          .select(
            lower($"word").alias(
              "word"
            ) // or else column will have new name "lower(word)"
          )
      lowerWords
        .groupBy("word")
        .count()
        .alias("count")
        .sort($"count".desc)

    // DataSet format
    wordsPipeline(words)
      .show(20)

    // DataSet + RDD format
    // non-structured data easier to load in RDD

    val bookRdd = spark.sparkContext.textFile("data/book.txt")
    // flatMap looks much easier than explode
    val wordsRdd = bookRdd.flatMap(l => l.split("\\W+"))
    val wordsDs: Dataset[String] = wordsRdd.toDS()

    // default name of column in RDD is value and wordsPipeline expects "word" name
    wordsPipeline(wordsDs.withColumnRenamed("value", "word").as[String])
      .show(20)

    spark.close()
