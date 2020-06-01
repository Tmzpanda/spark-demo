// Create DataFrame from different sources
// DataFrame Column and Row operations

package com.tmzpanda.spark.sparksql

import com.tmzpanda.spark.sparkutils.Context

object DataFrame_Functions extends App with Context {

  // Create DataFrame from Tuples
  val donuts = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
  val donutsDF = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Prices")
  donutsDF.show()


  // Get DataFrame column names
  val columns: Array[String] = donutsDF.columns
  columns.foreach(name => println(s"$name"))
  columns.contains("Prices")


  // DataFrame column names and types
  val (columnNames, columnDataTypes) = donutsDF.dtypes.unzip
  println(s"DataFrame column names = ${columnNames.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")


  // Split DataFrame Array column
  import sparkSession.implicits._
  val donutsDFSplit = donutsDF
    .select(
      $"Donut Name",
      $"Prices"(0).as("Low Price"),
      $"Prices"(1).as("High Price")
    )
  donutsDFSplit.show()


  // Rename DataFrame column
  donutsDFSplit.
    withColumnRenamed("Donut Name", "Donut Names")
    .show


  // Create DataFrame constant column
  import org.apache.spark.sql.functions._
  donutsDF
    .withColumn("Tasty", lit(true))
    .withColumn("Correlation", lit(1))
    .withColumn("Stock Min Max", typedLit(Seq(100, 500)))
    .show


  // Append column to DataFrame using withColumn()
  // DataFrame new column with User Defined Function (UDF)
  val stockMinMax: String => Seq[Int] = {
    case "Plain Donut" => Seq(100, 500)
    case "Vanilla Donut" => Seq(200, 400)
    case "Glazed Donut" => Seq(300, 600)
    case _ => Seq(150, 150)
  }

  val udfStockMinMax = udf(stockMinMax)
  donutsDF
    .withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
    .show()


  // Format DataFrame column

  val donutsSeq = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val donutsSeqDF = sparkSession.createDataFrame(donutsSeq).toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  donutsSeqDF
    .withColumn("Price Formatted", format_number($"Price", 2))
    .withColumn("Name Formatted", format_string("awesome %s", $"Donut Name"))
    .withColumn("Name Uppercase", upper($"Donut Name"))
    .withColumn("Name Lowercase", lower($"Donut Name"))
    .withColumn("Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
    .withColumn("Day", dayofmonth($"Purchase Date"))
    .withColumn("Month", month($"Purchase Date"))
    .withColumn("Year", year($"Purchase Date"))
    .show()


  // DataFrame First Row
  val firstRow = donutsDFSplit.first()
  println(s"First row = $firstRow")

  val firstRowColumn1 = firstRow.get(0)
  println(s"First row column 1 = $firstRowColumn1")

  val firstRowColumnPrice = firstRow.getAs[Double]("Low Price")
  println(s"First row column Price = $firstRowColumnPrice")


  // Json into DataFrame using explode()
  val tagsDFJSON = sparkSession
    .read
    .option("multiLine", value = true)
    .option("inferSchema", value = true)
    .json("src/main/resources/tags_sample.json")

  import org.apache.spark.sql.functions._
  val tagsDFExploded = tagsDFJSON.select(explode($"stackoverflow") as "stackoverflow_tags")
  tagsDFExploded.show()
  tagsDFExploded.printSchema()

  val tagsDF =  tagsDFExploded.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  )
  tagsDF.show()


  // Search DataFrame column using array_contains()
  tagsDF
    .select("*")
    .filter(array_contains($"frameworks_name","Play Framework"))
    .show()


  // Connect with database
  import java.util.Properties

  val connectionProperties = new Properties()
  connectionProperties.put("user", "root")
  connectionProperties.put("password", "xxxx")
  connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")
  connectionProperties.put("serverTimezone", "UTC")

  val jdbcDF = sparkSession.read
    .jdbc("jdbc:mysql://localhost:3306", "employees.dept_emp", connectionProperties)
    .toDF("Employee ID", "Department Code", "From Date", "To Date")
  jdbcDF.show(10)


  sparkSession.stop()
}
