package org.apache.spark

import org.apache.spark.sql.SparkSession

object TestWriteDriver {
  
  def main(args: Array[String]): Unit = {
    
    // spark-submit --deploy-mode cluster --master yarn --conf spark.driver.cores=1 
    // --class org.apache.spark.datasources.dynamodb.TestDriver testDynamoRead.jar <tablename>
    
    val spark = SparkSession
      .builder()
      .appName("Test Write DynamoDataSource")
      .getOrCreate()

    val dynamoDf = spark.read.parquet("s3://sie-nav-whs-dl-event-100-e1-pdx/100/event_year=2019/event_month=07/event_day=31")

    val readCount = dynamoDf.count()
    println("Read count : %d".format(readCount))

    dynamoDf.write
      .format("org.apache.spark.datasources.dynamodb.DynamodbDataSource")
      .option("dynamodb.capacity", 2000)
      .option("dynamodb.table.name", args(0))
      .option("dynamodb.table.region", "us-west-2").save()

  }
  
}