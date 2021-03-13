package com.bbn.streaming

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object ReadFromSocket extends App {

  val spark = SparkSession.builder()
    .appName("SampleStreaming")
    .master("local[2]")
    .getOrCreate()

  //  open terminal and use nc -lk 12345
  def readFromSocket = {
    val inputLinesFromSocket = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val out: Unit = inputLinesFromSocket.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def aggStreaming = {
    val inputLinesFromSocket = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount = inputLinesFromSocket.selectExpr("count(*) as lineCount")

    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggStreaming(function: Column => Column) = {
    val inputLinesFromSocket = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = inputLinesFromSocket.select(col("value").cast("integer").as("number"))
    val funcDf = numbers.select(function(col("number").as("agg_so_far")))

    funcDf.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  //  readFromSocket // Simple read call
  //  aggStreaming // Simple agg call
  aggStreaming(sum) // call with function passed

}
