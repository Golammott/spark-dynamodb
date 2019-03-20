package org.apache.spark.datasources.dynamodb

import org.apache.spark.sql.sources.v2.DataSourceOptions

class DynamodbDataSourceOptions(options: DataSourceOptions) {
  
  def getSegments(): Int = {
    options.getInt(DynamodbDataSourceOptions.segments, 1)
  }
  
  def getCapacity(): Int = {
    options.getInt(DynamodbDataSourceOptions.capacity, 100)
  }
}

object DynamodbDataSourceOptions {
  val segments = "dynamodb.read.segments";
  val capacity = "dynamodb.capacity";
}