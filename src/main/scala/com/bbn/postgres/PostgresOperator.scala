package com.bbn.postgres

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object PostgresOperator extends App {

  val spark = SparkSession.builder()
    .appName("PostgresOperator")
    .config("spark.master", "local")
    .getOrCreate()

  //  Postgres docker from rtjvm
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  //  max salary per employee
  val maxSalaryPerEmployee = salariesDF.groupBy("emp_no").agg(max("salary").as("MaxSalary"))
  val empMappedMaxSalary = employeesDF.join(maxSalaryPerEmployee, "emp_no")

  //  employees who were never manager
  val neverManagers = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"), "leftanti")

  //  job tittle of best paid 10 employees
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = maxSalaryPerEmployee.orderBy(col("MaxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()

}
