package com.tmzpanda.spark.streaming

import java.time.{LocalDate, Period}

import com.tmzpanda.spark.sparkutils.Context
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object StreamProcessor {
  def main(args: Array[String]): Unit = {

    new StreamProcessor("localhost:9092", Constants.personsTopic).process()

  }
}


class StreamProcessor(brokers: String, topics: String) extends Context {

  def process(): Unit = {

    // read stream
    val inputDf = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()

    val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    // pattern matching
    val struct = new StructType()
      .add("firstName", DataTypes.StringType)
      .add("lastName", DataTypes.StringType)
      .add("birthDate", DataTypes.StringType)
    import sparkSession.implicits._
    val personNestedDf = personJsonDf.select(from_json($"value", struct).as("person"))

    // DataFrame operation
    val personFlattenedDf = personNestedDf.select($"person.firstName", $"person.lastName", $"person.birthDate")
    val personDf = personFlattenedDf.withColumn("birthDate", to_timestamp($"birthDate", "yyyy-MM-dd'T'HH:mm:ss"))

    val ageFunc: java.sql.Timestamp => Int = birthDate => {
      val birthDateLocal = birthDate.toLocalDateTime.toLocalDate
      val age = Period.between(birthDateLocal, LocalDate.now()).getYears
      age
    }
    val ageUdf: UserDefinedFunction = udf(ageFunc, DataTypes.IntegerType)
    val processedDf = personDf.withColumn("age", ageUdf($"birthDate"))

    val resDf = processedDf.select(
      concat($"firstName", lit(" "), $"lastName").as("key"),
      processedDf.col("age").cast(DataTypes.StringType).as("value"))

    // write stream to console
    val consoleOutput = resDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // write stream to Kafka
    val kafkaOutput = resDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", "Streaming_ages")
      .option("checkpointLocation", "/Users/tianming/Downloads/checkpoints")
      .start()

    sparkSession.streams.awaitAnyTermination()

  }
}

