package com.bbn.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  val path = "/home/Desktop/"
  val inputGuitarFile = "guitarPlayers.json"
  val inputBandsFile = "bands.json"

  val spark = SparkSession.builder()
    .appName("SampleStreamingJoins")
    .master("local[2]")
    .getOrCreate()

  def readDf(jsonPath: String) = {
    spark.read
      .option("inferSchema", "true")
      .json(s"$jsonPath")
  }

  val guitarPlayersDf = readDf(s"$path/$inputGuitarFile")
  val bandsDf = readDf(s"$path/$inputBandsFile")
  val bandsSchema = bandsDf.schema

  def joinStreamsAndStatic() = {
    val streamedDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("Band"))
      .selectExpr("Band.id as id", "Band.name as name", "Band.hometown as hometown", "band.year as year")

    val joinedDf = streamedDf.join(guitarPlayersDf, guitarPlayersDf.col("Band") === streamedDf.col("id"), "inner")

    joinedDf.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  joinStreamsAndStatic()

}
