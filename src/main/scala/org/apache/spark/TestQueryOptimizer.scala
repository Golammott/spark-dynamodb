package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

case class Event(uid: String, event_id: Integer, first: Integer, second: Integer)

object TestQueryOptimizer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Test Read DynamoDataSource")
      .getOrCreate()

    import spark.implicits._

    val testSeq = Seq(("somestuff", 1,2,3), ("somestuff", 1,2,3), ("somestuff", 1,2,3)).toDS()
    println(testSeq.schema)
    //testSeq.filter(testSeq.col)
    //testSeq.logicalPlan.


//    val dynamoDf = spark.read.format("org.apache.spark.datasources.dynamodb.DynamodbDataSource")
//      .option("dynamodb.read.segments", 100)
//      .option("dynamodb.capacity", 4000)
//      .option("dynamodb.table.name", args(0))
//      .option("dynamodb.table.region", "us-west-2")
//      .load()
  }

}
