package org.apache.spark.datasources.dynamodb

import org.apache.spark.sql.{SparkSession, Row, Encoders}

class TestDriver {
  val spark = SparkSession
      .builder()
      .appName("Reload Table")
      .getOrCreate()
      
  //val dynamoDf = spark.read.
  //val dynamoDf = spark.read.format("org.apache.spark.datasources.dynamodb.DynamodbDataSource").load()
}