package org.apache.spark.datasources.dynamodb

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DescribeTableRequest, DescribeTableResponse}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region

class DynamoClient(region: String) {
  
  val ddb = DynamoDbAsyncClient.builder().credentialsProvider(DefaultCredentialsProvider.builder().build()).region(Region.of(region)).build()
  
  def getTable(): DescribeTableResponse = {
    val tableDDL = ddb.describeTable(DescribeTableRequest.builder().tableName("").build()).get()
    tableDDL
  }
}