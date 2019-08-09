package org.apache.spark

import java.text.AttributedCharacterIterator.Attribute

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.DynamoRdd
import org.apache.spark.sql.types.{StructField, StructType, StringType, MapType, LongType, IntegerType, DataType, BinaryType, BooleanType, DoubleType}

//import java.util.Map
import scala.collection.{mutable => sm}

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
  
import scala.collection.JavaConverters._

object TestRddDriver {
  
//  implicit class DynamoSparkContext(context: SparkContext) {
//
//    def dynamoRdd(): DynamoRdd[sm.Map[String, AttributeValue]] = context.withScope {
//      val f: (java.util.Map[String, AttributeValue]) => sm.Map[String, Any] = y => {
//
////        def toSparkType(attributeTypeAsString: String): StructType = field match {
////          case "B" => BinaryType
////          case "S" => StringType
////          case "N" => DoubleType
////        }
//
//        def fromAttributeValue(dataType: DataType, attr: Option[AttributeValue]): Any = dataType match {
//          case s: StringType => if (attr.isDefined) attr.get.s() else null
//          case i: IntegerType => if (attr.isDefined) attr.get.n().toInt else null
//          case m: MapType => if (attr.isDefined) attr.get.m().asScala.mapValues(f => f.s()) else null
//          case l: LongType => if (attr.isDefined) attr.get.n().toLong else null
//          case d: DoubleType => if (attr.isDefined) attr.get.n().toDouble else null
//        }
//
//        val scalaMap = y.asScala
////        scalaMap.map({case (key, value) => (key, fromAttributeValue(value.))})
//      }
//      new DynamoRdd(context, f)
//    }
//  }
  
  def main(args: Array[String]): Unit = {
    
    // spark-submit --deploy-mode cluster --master yarn --conf spark.driver.cores=1 
    // --class org.apache.spark.datasources.dynamodb.TestDriver testDynamoRead.jar <tablename>
    
    val spark = SparkSession
      .builder()
      .appName("Test DynamoDataSource")
      .config("region", "us-west-2")
      .config("table", args(0))
      .config("segments", 100)
      .getOrCreate()

    val dynamoDf = spark.read.format("org.apache.spark.datasources.dynamodb.DynamodbDataSource").load("")
    
//    spark.conf.set("region", "us-west-2")
//    val confff = spark.conf
//    val dynamoDf = spark.sparkContext.dynamoRdd()

//      .option("dynamodb.read.segments", 100)
//      .option("dynamodb.capacity", 2000)
//      .option("dynamodb.table.name", args(0))
//      .option("dynamodb.table.region", "us-west-2")
//      .load()
      
    println("Count after completed read : " + dynamoDf.count())
  }
  
}