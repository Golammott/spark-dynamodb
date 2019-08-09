package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

class DynamodbWriterCommitMessage(msg: String) extends WriterCommitMessage {
  
}