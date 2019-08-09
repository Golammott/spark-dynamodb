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
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.spark.internal.Logging


class DynamodbDataSource extends DataSourceV2 with ReadSupport with WriteSupport with DataSourceRegister with Logging {


  
  def createReader(options: DataSourceOptions): DataSourceReader = {

    logInfo("Creating Dynamo Reader")
    val dynamoOptions = new DynamodbDataSourceOptions(options)

    options.asMap().asScala.map( { case (k,v) => logInfo(s"Found key : $k, value : $v") })
    new DynamodbDataSourceReader(dynamoOptions)
  }
  
  def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    logInfo("Creating Dynamo Writer")
    val dynamoOptions = new DynamodbDataSourceOptions(options)

    Optional.of(new DynamodbDataSourceWriter(dynamoOptions.table, dynamoOptions.region, schema))
  }
  
  override def shortName(): String = {
    "dynamodb"
  }
  
}