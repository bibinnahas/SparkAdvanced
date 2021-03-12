package com.bbn.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LagExample extends App {

  val path = "/home/Desktop"
  val input_csv = "cars.json"

  val spark = SparkSession.builder()
    .appName("LagAnalyticalFuncTest")
    .config("spark.master", "local")
    .getOrCreate()

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"$path/$filename")

  case class CarsGrouped(
                          Name: String,
                          Miles_per_Gallon: Option[Double],
                          Cylinders: Long,
                          Displacement: Double,
                          Horsepower: Option[Long],
                          Weight_in_lbs: Long,
                          Acceleration: Double,
                          Year: String,
                          Origin: String,
                          year_from_date: String
                        )

  import spark.implicits._

  val cars = readDF(input_csv)
    .withColumn("year_from_date", substring(col("Year"), 1, 4))
    .as[CarsGrouped]
    .groupByKey(_.year_from_date)
    .count()
    .withColumnRenamed("count(1)", "TotalCarsInYear")
    .withColumnRenamed("key", "Year")

  val w = org.apache.spark.sql.expressions.Window.orderBy("Year")

  cars
    .withColumn("difference", lag("TotalCarsInYear", 1, 0).over(w))
    .withColumn("DiffFromPrevYear", col("difference")- col("TotalCarsInYear"))
    .drop(col("difference"))
    .show()

}
