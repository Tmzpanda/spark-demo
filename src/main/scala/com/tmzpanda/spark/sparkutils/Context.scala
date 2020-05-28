package com.tmzpanda.spark.sparkutils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


trait Context {

  lazy val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-basics")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")


  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)
}
