package org.apache.spark.datasources.dynamodb

import org.apache.spark.sql.sources.v2.DataSourceOptions
import java.util.function.{Function, Predicate, BiPredicate, BiConsumer}
import org.apache.spark.internal.Logging
import scala.collection.JavaConverters.mapAsScalaMapConverter

class DynamodbDataSourceOptions(options: DataSourceOptions) extends Logging with Serializable {
  
  logInfo("Debugging DynamodbDataSourceOptions in cstr")
  options.asMap().asScala.map( { case (k,v) => logInfo(s"Found key : $k, value : $v") })
//  val segments = options.getInt(DynamodbDataSourceOptions.DYNAMO_SEGMENTS, 1)
//  val capacity =
//  val table = options.get(DynamodbDataSourceOptions.DYNAMO_TABLE).get
//  val region = options.get(DynamodbDataSourceOptions.DYNAMO_REGION).get
  
  def segments: Int = options.getInt(DynamodbDataSourceOptions.DYNAMO_SEGMENTS, 1)
  def capacity: Int = options.getInt(DynamodbDataSourceOptions.DYNAMO_CAPACITY, 100)
  def table: String = options.get(DynamodbDataSourceOptions.DYNAMO_TABLE).get
  def region: String = options.get(DynamodbDataSourceOptions.DYNAMO_REGION).get
//    segments
//  }
  
//  def getCapacity(): Int = {
//    capacity
//  }
//
//  def getTable(): String = {
//    table
//    //.orElseThrow(throw new RuntimeException("Missing Required parameter : " + DynamodbDataSourceOptions.DYNAMO_TABLE))
//  }
//
//  def getRegion(): String = {
//    region
//    //.orElseThrow(throw new RuntimeException("Missing Required parameter : " + DynamodbDataSourceOptions.DYNAMO_REGION))
//  }
}

object DynamodbDataSourceOptions {
  
  val DYNAMO_SEGMENTS = "dynamodb.read.segments";
  val DYNAMO_CAPACITY = "dynamodb.capacity";
  val DYNAMO_TABLE = "dynamodb.table.name";
  val DYNAMO_REGION = "dynamodb.table.region";
  //val DYNAMO_REGION = "dynamodb.table.region";
  
}