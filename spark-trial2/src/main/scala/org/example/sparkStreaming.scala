package org.example
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.relaxng.datatype.Datatype

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
object sparkStreaming extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkStreaming")
    .getOrCreate();

  spark.sparkContext.setLogLevel("ERROR")
  println("Streaming starting")

  val initDF = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    )

  println("Streaming DataFrame : " + initDF.isStreaming)


  val resultDF = initDF
    .withColumn("result", col("value") + lit(1))

  resultDF
    .writeStream
    .outputMode("append")
    .option("truncate", false)
    .format("console")
    .start()
    .awaitTermination()

}