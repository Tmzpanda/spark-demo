package com.tmzpanda.spark.sparksql

import com.tmzpanda.spark.sparkutils.Context
import org.apache.spark.sql.Dataset

object Dataset extends App with Context {

  // Setup
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", value = false)
    .option("inferSchema", value = true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)


  // 1. Convert DataFrame row to Scala case class
  case class Tag(id: Int, tag: String)

  import sparkSession.implicits._
  val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]
  dfTagsOfTag
    .take(10)
    .foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))


  // 2. DataFrame row to Scala case class using map()
  case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)

  // create a function which will parse each element in the row
  def toQuestion(row: org.apache.spark.sql.Row): Question = {
    // to normalize our owner_user_id data
    val IntOf: String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }

    import java.time._
    val DateOf: String => java.sql.Timestamp = {
      s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
    }

    Question (
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creationDate = DateOf(row.getString(2)),
      score = row.getString(3).toInt
    )
  }

  // now let's convert each row into a Question case class
  import sparkSession.implicits._
  val dfOfQuestion: Dataset[Question] = dfQuestions.map(row => toQuestion(row))
  dfOfQuestion
    .take(10)
    .foreach(q => println(s"owner user id = ${q.owner_userid}, tag = ${q.tag}, creation date = ${q.creationDate}, score = ${q.score}"))








}
