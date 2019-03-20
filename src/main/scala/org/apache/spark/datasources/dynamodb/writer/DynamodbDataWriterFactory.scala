package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.DataWriter;

class DynamodbDataWriterFactory extends DataWriterFactory[InternalRow] {

    override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
        return new DynamodbDataWriter(); // take options as parameter
    }
  
}