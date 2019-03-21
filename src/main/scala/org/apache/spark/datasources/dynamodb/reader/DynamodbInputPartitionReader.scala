package org.apache.spark.datasources.dynamodb.reader

import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import java.io.IOException
import org.apache.spark.datasources.dynamodb.{DynamoClient, DynamodbDataSourceOptions}
import software.amazon.awssdk.services.dynamodb.model.{ScanResponse, AttributeValue}
//import java.util.Map

class DynamodbInputPartitionReader(options: DynamodbDataSourceOptions, segment: Int, totalSegments: Int) extends InputPartitionReader[InternalRow] {
  
  val ddbClient = new DynamoClient(options.getRegion(), options.getTable())
  var lastEvaluatedKey: java.util.Map[String, AttributeValue] = _
  var lastResultBlock: java.util.Iterator[java.util.Map[String, AttributeValue]] = _
  
  @throws(classOf[IOException])
  def next(): Boolean = {
    lastResultBlock.hasNext() || (lastEvaluatedKey != null)
  }
  
  def get(): InternalRow = {
    if (lastEvaluatedKey != null) {
      val scanResult = ddbClient.scan(segment, totalSegments).get
      lastEvaluatedKey = scanResult.lastEvaluatedKey()
      lastResultBlock = scanResult.items().iterator()
    }
    
    lastResultBlock.next()
    InternalRow()
  }
  
  def close() = {
    
  }
  
}