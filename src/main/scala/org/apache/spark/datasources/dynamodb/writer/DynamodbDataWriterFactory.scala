package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.datasources.dynamodb.DynamoClient
import org.apache.spark.sql.types.StructType


class DynamodbDataWriterFactory(table: String, region: String, schema: StructType) extends DataWriterFactory[InternalRow] {

    override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
        return new DynamodbDataWriter(table, region, schema); // take options as parameter
    }
  
}