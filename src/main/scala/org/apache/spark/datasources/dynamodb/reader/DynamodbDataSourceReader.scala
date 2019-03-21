package org.apache.spark.datasources.dynamodb.reader

import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition};
import software.amazon.awssdk.services.dynamodb.model.{DescribeTableRequest, DescribeTableResponse}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.datasources.dynamodb.{DynamodbDataSourceOptions, DynamoClient}
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConverters._

class DynamodbDataSourceReader(options: DynamodbDataSourceOptions) extends DataSourceReader {
  
  def readSchema(): StructType = {
    // TODO get the user defined schema instead of defaulting to only keys for the table
    val ddbClient = new DynamoClient(options.getRegion(), options.getTable())
    
    ddbClient.getDefaultSchema()
  }

  def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    val totalSegments = options.getSegments()
    (0 until totalSegments).map(segmentNumber => { 
      new DynamodbInputPartition(options, segmentNumber, totalSegments): InputPartition[InternalRow]
    }).toList.asJava 

  }
}