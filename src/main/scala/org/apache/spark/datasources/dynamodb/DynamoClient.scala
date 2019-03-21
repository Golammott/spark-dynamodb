package org.apache.spark.datasources.dynamodb

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DescribeTableRequest, DescribeTableResponse, AttributeValue, AttributeDefinition, ScanRequest, ScanResponse}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region

import org.apache.spark.sql.types.{StructField, StructType, StringType, MapType, LongType, IntegerType, DataType, BinaryType, BooleanType, DoubleType}

import scala.collection.JavaConverters._

class DynamoClient(val region: String, val table: String) {
  
  val ddb = DynamoDbAsyncClient.builder().credentialsProvider(DefaultCredentialsProvider.builder().build()).region(Region.of(region)).build()
  
  def getDefaultSchema(): StructType = {
    
    val tableDDL = ddb.describeTable(DescribeTableRequest.builder().tableName(table).build()).get()
    
    val structFields = tableDDL.table().attributeDefinitions().asScala.map(f => {
      f.attributeTypeAsString()
      StructField(f.attributeName(), DynamoClient.toSparkType(f.attributeTypeAsString()))
    })
    new StructType(structFields.toArray)
  }
  
  private def getProjectionExpression(): String = {
    getDefaultSchema().fields.map(f => f.name).mkString(",")
  }
  
  def scan(segment: Int, totalSegments: Int) = {
    val scanRequest = ScanRequest.builder()
      .tableName(table)
      .projectionExpression(getProjectionExpression())
      .segment(segment)
      .totalSegments(totalSegments).build()
    ddb.scan(scanRequest)
  }
}

object DynamoClient {
  def toSparkType(field: String): DataType = {
    field match {
      case "B" => BinaryType
      case "S" => StringType
      case "N" => DoubleType
    }
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