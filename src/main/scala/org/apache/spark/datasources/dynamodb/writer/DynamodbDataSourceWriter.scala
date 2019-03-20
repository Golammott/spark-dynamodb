package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

class DynamodbDataSourceWriter extends DataSourceWriter {
  
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new DynamodbDataWriterFactory()
  }
  
  override def commit( messages: Array[WriterCommitMessage]) = {
    
  }
  
  override def abort(messages: Array[WriterCommitMessage]) = {
    
  }
  
}