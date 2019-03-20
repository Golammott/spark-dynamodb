package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import java.io.IOException

class DynamodbDataWriter extends DataWriter[InternalRow] {
  
  // this is responsible for actually doing the writes in each partition/task
  // here we should create the dynamodb client and publish writes 
  
  @throws(classOf[IOException])
  override def write(record: InternalRow) = {
    
  }
  
  @throws(classOf[IOException])
  override def commit(): WriterCommitMessage = {
    new DynamodbWriterCommitMessage()
  }
  
  @throws(classOf[IOException])
  override def abort() = {
    
  }
  
}