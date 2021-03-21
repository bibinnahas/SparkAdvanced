package com.bbn.ds

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object DatasetTest extends App {

  val path = "/home/***/Desktop/"

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$path/numbers.csv")

  numbersDF.printSchema()

  //  convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numberDs: Dataset[Int] = numbersDF.as[Int]

  numberDs.show()
  //  simple predicates are possible in DS
  numberDs.filter(_ < 100).show()

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"$path/$filename")

  val carsDF = readDF("cars.json")

  import spark.implicits._

  val carsDS = carsDF.as[Car]

  carsDS.map(car => car.Name.toUpperCase()).show()

  //  Cars with power
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  carsDS.groupByKey(_.Origin).count().show()

  carsDS.select(col("Name")).show(10, false)

  println(carsDF.columns)

}
