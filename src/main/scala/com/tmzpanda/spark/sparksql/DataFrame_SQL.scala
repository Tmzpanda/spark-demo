// Compare spark DataFrame interface with SQL query

package com.tmzpanda.spark.sparksql


import com.tmzpanda.spark.sparkutils.Context


object DataFrame_SQL extends App with Context {


  // Create a DataFrame from reading a CSV file
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)


  // Print DataFrame schema
  dfTags.printSchema()


  // Query DataFrame: select columns from a DataFrame
  dfTags.select("id", "tag").show(10)


  // DataFrame Query: Distinct
  dfTags
    .select("tag")
    .distinct()
    .show(10)


  // DataFrame Query: filter by column value of a DataFrame
  dfTags.filter("tag == 'php'").show(10)


  // DataFrame Query: count rows of a DataFrame
  println(s"Number of php tags = ${ dfTags.filter("tag == 'php'").count() }")


  // DataFrame Query: SQL like query
  dfTags.filter("tag like 's%'").show(10)


  // DataFrame Query: Multiple filter chaining
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)


  // DataFrame Query: SQL IN clause
  dfTags.filter("id in (25, 108)").show(10)


  // DataFrame Query: SQL Group By
  println("Group by tag value")
  dfTags.groupBy("tag").count().show(10)


  // DataFrame Query: SQL Group By with filter
  dfTags.groupBy("tag").count().filter("count > 5").show(10)


  // DataFrame Query: SQL order by
  dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)


  // DataFrame Query: Cast columns to specific data type
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_user_id", "answer_count")

  dfQuestionsCSV.printSchema()

  import sparkSession.implicits._
  val dfQuestions = dfQuestionsCSV.select(
    $"id".cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_user_id").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  dfQuestions.printSchema()
  dfQuestions.show(10)


  // DataFrame Query: Operate on a sliced DataFrame
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410")
  dfQuestionsSubset.show()


  // statistics
  import org.apache.spark.sql.functions._
  dfQuestions
    .select(avg("score"), max("score"), sum("score"))
    .show()


  // Group by with statistics
  dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_user_id is not null")
    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions.col("owner_user_id"))             // -> org.apache.spark.sql.RelationalGroupedDataset
    .agg(avg("score"), max("answer_count"))
    .show()


  // DataFrame Query: Join
  dfQuestionsSubset.join(dfTags, "id").show(10)


  // DataFrame Query: Join and select columns
  dfQuestionsSubset
    .join(dfTags, "id")
    .select("owner_user_id", "tag", "creation_date", "score")
    .show(10)


  // DataFrame Query: Join on explicit columns
  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .show(10)


  // DataFrame Query: Inner Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)


  // DataFrame Query: Left Outer Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)


  // DataFrame Query: Right Outer Join
  dfTags
    .join(dfQuestionsSubset, Seq("id"), "right_outer")
    .show(10)


  // Create DataFrame from collection
  val seqTags = Seq(
    1 -> "so_java",
    1 -> "so_jsp",
    2 -> "so_erlang",
    3 -> "so_scala",
    3 -> "so_akka"
  )

  import sparkSession.implicits._
  val dfMoreTags = seqTags.toDF("id", "tag")
  dfMoreTags.show(10)


  // DataFrame Union
  val dfUnionOfTags = dfTags
    .union(dfMoreTags)
    .filter("id in (1,3)")
  dfUnionOfTags.show(10)


  // DataFrame Intersection
  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionOfTags)
  dfIntersectionTags.show(10)


  // Append column to DataFrame using withColumn()
  import org.apache.spark.sql.functions._
  val dfSplitColumn = dfMoreTags
    .withColumn("tmp", split($"tag", "_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(1).as("so_tag")
    ).drop("tmp")
  dfSplitColumn.show(10)


  sparkSession.stop()
}


