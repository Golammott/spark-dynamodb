package org.apache.spark.datasources.dynamodb

import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.datasources.dynamodb.reader.DynamodbDataSourceReader
import org.apache.spark.datasources.dynamodb.writer.DynamodbDataSourceWriter
import org.apache.spark.datasources.dynamodb.writer.DynamodbDataSourceWriter
import java.util.Optional


class DynamodbDataSource extends DataSourceV2 with ReadSupport with WriteSupport with DataSourceRegister {
  
  def createReader(options: DataSourceOptions): DataSourceReader = {
    val dynamoOptions = new DynamodbDataSourceOptions(options)
    new DynamodbDataSourceReader(dynamoOptions)
  }
  
  def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new DynamodbDataSourceWriter())
  }
  
  override def shortName(): String = {
    "dynamodb"
  }
  
}