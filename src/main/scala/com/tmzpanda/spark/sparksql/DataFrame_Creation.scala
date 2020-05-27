package com.tmzpanda.spark.sparksql

import com.tmzpanda.spark.sparkutils.Context

object DataFrame_Creation extends App with Context {

  // Create DataFrame from Tuples
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")
  df.show()

  val columns: Array[String] = df.columns
  columns.foreach(name => println(s"$name"))

  val (columnNames, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names = ${columnNames.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")


  // Json into DataFrame using explode()
  val tagsDF = sparkSession
    .read
    .option("multiLine", value = true)
    .option("inferSchema", value = true)
    .json("src/main/resources/tags_sample.json")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._
  val df = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")














}
