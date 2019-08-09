package org.apache.spark

import org.apache.spark.sql.SparkSession

object TestReadDriver {
  
  def main(args: Array[String]): Unit = {
    
    // spark-submit --deploy-mode cluster --master yarn --conf spark.driver.cores=1 
    // --class org.apache.spark.datasources.dynamodb.TestDriver testDynamoRead.jar <tablename>
    
    val spark = SparkSession
      .builder()
      .appName("Test Read DynamoDataSource")
      .getOrCreate()
      
    val dynamoDf = spark.read.format("org.apache.spark.datasources.dynamodb.DynamodbDataSource")
      .option("dynamodb.read.segments", 100)
      .option("dynamodb.capacity", 2000)
      .option("dynamodb.table.name", args(0))
      .option("dynamodb.table.region", "us-west-2")
      .load()
      
    println("Count after completed read : " + dynamoDf.count())
  }
  
}