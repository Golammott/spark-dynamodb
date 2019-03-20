package org.apache.spark.datasources.dynamodb.reader

import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader};
import org.apache.spark.sql.catalyst.InternalRow;

class DynamodbInputPartition(segment: Int, totalSegments: Int) extends InputPartition[InternalRow] {
  
   override def createPartitionReader(): InputPartitionReader[InternalRow] = {
     new DynamodbInputPartitionReader()
   }
  
}