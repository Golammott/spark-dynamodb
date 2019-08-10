package org.apache.spark.datasources.dynamodb

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DescribeTableRequest, DescribeTableResponse, AttributeValue, AttributeDefinition, ScanRequest, ScanResponse, ReturnConsumedCapacity}
import software.amazon.awssdk.services.dynamodb.model.WriteRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient


import org.apache.spark.sql.types.{StructField, StructType, StringType, MapType, LongType, IntegerType, DataType, BinaryType, BooleanType, DoubleType}

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging

class DynamoClient(val table: String, val region: String) extends Logging {
  
  val ddb = DynamoDbAsyncClient.builder()
    .credentialsProvider(DefaultCredentialsProvider.builder().build())
    .region(Region.of(region))
    .httpClientBuilder(NettyNioAsyncHttpClient.builder()
    .maxConcurrency(100)
    .maxPendingConnectionAcquires(10))
    .build()
  
  def getDefaultSchema(): StructType = {

    logInfo("Getting schema for table : %s".format(table))
    
    val tableDDL = ddb.describeTable(DescribeTableRequest.builder().tableName(table).build()).get()
    
    val structFields = tableDDL.table().attributeDefinitions().asScala.map(f => {
      f.attributeTypeAsString()
      StructField(f.attributeName(), DynamoClient.toSparkType(f.attributeTypeAsString()))
    })
    new StructType(structFields.toArray)
  }
  
  def close() = {
    ddb.close()
  }
  
  private def getProjectionExpression(): String = {
    getDefaultSchema().fields.map(f => f.name).mkString(",")
  }
  
  private def doScan(scanRequest: ScanRequest.Builder): ScanResponse = {
    ddb.scan(scanRequest.build()).get()
  }

  def writeBatch(): Unit = {
    // TODO
    throw new RuntimeException("Batch write not implemented")
  }

  def write(item: java.util.Map[String, AttributeValue]): Unit = {
    val putRequest = PutItemRequest.builder().tableName(table).item(item).build()
    ddb.putItem(putRequest).get()
  }
  
  def scan(segment: Int, totalSegments: Int): ScanResponse = {
    val scanRequest = ScanRequest.builder()
      .tableName(table)
      .projectionExpression(getProjectionExpression())
      .segment(segment)
      .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
      .totalSegments(totalSegments)
    doScan(scanRequest)
  }
  
  def scanTable(segment: Int, totalSegments: Int): java.util.Iterator[java.util.Map[String, AttributeValue]] = {
    logInfo(s"Starting scan request segment : $segment/$totalSegments")
    val scanRequest = ScanRequest.builder()
      .tableName(table)
      .projectionExpression(getProjectionExpression())
      .segment(segment)
      .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
      .totalSegments(totalSegments)
    val result = doScan(scanRequest)
    result.items().iterator()
  }
  
  def scan(segment: Int, totalSegments: Int, lastKey: java.util.Map[String, AttributeValue]): ScanResponse = {
    val scanRequest = ScanRequest.builder()
      .tableName(table)
      .exclusiveStartKey(lastKey)
      .projectionExpression(getProjectionExpression())
      .segment(segment)
      .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
      .totalSegments(totalSegments)
    doScan(scanRequest)
  }
}

object DynamoClient {
  def toSparkType(field: String): DataType = {
    // Dynamo keys must be one of these three types
    field match {
      case "B" => BinaryType
      case "S" => StringType
      case "N" => DoubleType
    }
  }
  
  def fromAttributeValue(dataType: DataType, attr: Option[AttributeValue]): Any = dataType match {
      case s: StringType => if (attr.isDefined) attr.get.s() else null
      case i: IntegerType => if (attr.isDefined) attr.get.n().toInt else null
      case m: MapType => if (attr.isDefined) attr.get.m().asScala.mapValues(f => f.s()) else null
      case l: LongType => if (attr.isDefined) attr.get.n().toLong else null
      case d: DoubleType => if (attr.isDefined) attr.get.n().toDouble else null
  }
  
  def toSparkNumeric(value: AnyVal): DataType = {
    value match {
      case i: Int => IntegerType
      case l: Long => LongType
      case d: Double => DoubleType
    }
  }
  
  def toBestNumber(number: String): AnyVal = {
    try { return number.toInt } catch { case nfe: NumberFormatException => println("FormatException thrown") }
    try { return number.toLong } catch { case nfe: NumberFormatException => println("FormatException thrown") }
    try { return number.toDouble } catch { case nfe: NumberFormatException => println("FormatException thrown") }
  }
}