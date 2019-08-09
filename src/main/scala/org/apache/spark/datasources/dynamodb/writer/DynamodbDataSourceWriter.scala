package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.datasources.dynamodb.DynamoClient
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.apache.spark.sql.types.StructType;

class DynamodbDataSourceWriter(val table: String, val region: String, val schema: StructType) extends DataSourceWriter {
  
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {

    new DynamodbDataWriterFactory(table, region, schema)
  }
  
  override def commit( messages: Array[WriterCommitMessage]) = {
    
  }
  
  override def abort(messages: Array[WriterCommitMessage]) = {
    
  }
  
}