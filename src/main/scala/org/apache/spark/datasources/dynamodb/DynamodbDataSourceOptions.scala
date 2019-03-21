package org.apache.spark.datasources.dynamodb

import org.apache.spark.sql.sources.v2.DataSourceOptions
//import java.

class DynamodbDataSourceOptions(options: DataSourceOptions) {
  
  def getSegments(): Int = {
    options.getInt(DynamodbDataSourceOptions.segments, 1)
  }
  
  def getCapacity(): Int = {
    options.getInt(DynamodbDataSourceOptions.capacity, 100)
  }
  
  def getTable(): String = {
    options.get(DynamodbDataSourceOptions.table).orElseThrow(throw new RuntimeException("Missing Required parameter : " + DynamodbDataSourceOptions.table))
  }
  
  def getRegion(): String = {
    options.get(DynamodbDataSourceOptions.region).orElseThrow(throw new RuntimeException("Missing Required parameter : " + DynamodbDataSourceOptions.region))
  }
}

object DynamodbDataSourceOptions {
  val segments = "dynamodb.read.segments";
  val capacity = "dynamodb.capacity";
  val table = "dynamodb.table.name";
  val region = "dynamodb.table.region";
}