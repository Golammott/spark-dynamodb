package org.apache.spark.datasources.dynamodb.reader

import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import java.io.IOException

class DynamodbInputPartitionReader extends InputPartitionReader[InternalRow] {
  
  @throws(classOf[IOException])
  def next(): Boolean = {
    true
  }
  
  def get(): InternalRow = {
    InternalRow()
  }
  
}