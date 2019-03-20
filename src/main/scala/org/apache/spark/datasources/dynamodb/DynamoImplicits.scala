package org.apache.spark.datasources.dynamodb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader

object DynamoImplicits {
  
  implicit class DynamoDBDataFrameReader(reader: DataFrameReader) {

    /** Read a DynamoDB table into a [[DataFrame]].
      *
      * This adds a `dynamodb` method to [[DataFrameReader]] that allows you to read a
      * DynamoDB table into a DataFrame as follows:
      *
      * {{{
      *   val df = sqlContext.read.dynamodb("users")
      * }}}
      *
      * @param table DynamoDB table name to scan.
      * @return DataFrame with records scanned from the DynamoDB table.
      */
    def dynamodb(table: String): DataFrame =
      reader
        .format("com.github.traviscrawford.spark.dynamodb")
        .option("table", table)
        .load
  
    /** Read a DynamoDB table into a [[DataFrame]].
      *
      * This adds a `dynamodb` method to [[DataFrameReader]] that allows you to read a
      * DynamoDB table into a DataFrame as follows:
      *
      * {{{
      *   val df = sqlContext.read.dynamodb("us-west-2", "users")
      * }}}
      *
      * @param region AWS region containing the DynamoDB table to scan.
      * @param table DynamoDB table name to scan.
      * @return DataFrame with records scanned from the DynamoDB table.
      */
    def dynamodb(region: String, table: String): DataFrame =
      reader
        .format("com.github.traviscrawford.spark.dynamodb")
        .option("region", region)
        .option("table", table)
        .load
  }
  
}


