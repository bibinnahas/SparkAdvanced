package com.bbn.analytics

import com.datastax.spark.connector.mapper.DataFrameColumnMapper
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DenseRank extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()


  val path = "/home/thesnibibin/Desktop/LearningSparkV2/databricks-datasets/learning-spark-v2/flights"
  val delayPath = s"$path/departuredelays.csv"
  val airportCodePath = s"$path/airport-codes-na.txt"

  spark.sql("create database bibin")
  spark.sql("use bibin")
  val databasesDF = spark.sql("show databases")
  val tablesDB = spark.sql("show tables")

  val airports = spark.read
    .option("header", "true")
    .option("inferschema", "true")
    .option("delimiter", "\t")
    .csv(airportCodePath)
  airports.createOrReplaceTempView("airports_na")

  def writeToDb(df: DataFrame, tableName: String) = {
    df.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  val delays = spark.read
    .option("header", "true")
    .csv(delayPath)
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance"))
  delays.createOrReplaceTempView("departureDelays")

  writeToDb(delays, "departureDelays")
  writeToDb(airports, "airports_na")

  val fromDelays = spark.sql(
    """
      |SELECT origin, destination, SUM(delay) AS TotalDelays
      |FROM departureDelays
      |WHERE origin IN ('SEA', 'SFO', 'JFK')
      |AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
      |GROUP BY origin, destination;
      |""".stripMargin)

  writeToDb(fromDelays, "departureDelaysWindow")

  spark.sql(
    """SELECT origin, destination, TotalDelays, rank
      |FROM (
      |SELECT origin, destination, TotalDelays, dense_rank()
      |OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
      |FROM departureDelaysWindow
      |) t
      |WHERE rank <= 3""".stripMargin).show()


}
