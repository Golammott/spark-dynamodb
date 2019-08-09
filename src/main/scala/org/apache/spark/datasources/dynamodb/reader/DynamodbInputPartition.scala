package org.apache.spark.datasources.dynamodb.reader

import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader};
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.datasources.dynamodb.DynamodbDataSourceOptions
import org.apache.spark.sql.types.{StructField, StructType}

class DynamodbInputPartition(table: String, region: String, capacity: Int, segment: Int, totalSegments: Int, schema: StructType) extends InputPartition[InternalRow] {
  
   override def createPartitionReader(): InputPartitionReader[InternalRow] = {
     new DynamodbInputPartitionReader(table, region, capacity, segment, totalSegments, schema)
   }
  
}