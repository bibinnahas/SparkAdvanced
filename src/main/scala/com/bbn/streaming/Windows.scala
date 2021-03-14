package com.bbn.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object Windows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def finalWindowing() = {
    val result = readPurchasesFromSocket()
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("Time"))
      .agg(sum("quantity").as("TotalQuantityPerHour"))
      .select(
        col("Time").getField("start").as("start"),
        col("Time").getField("end").as("end"),
        col("item"),
        col("TotalQuantityPerHour")
      )

    result.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    finalWindowing()
  }

}
