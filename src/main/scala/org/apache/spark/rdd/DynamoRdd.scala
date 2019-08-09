package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator

import org.apache.spark.datasources.dynamodb.DynamoClient
import software.amazon.awssdk.services.dynamodb.model.{DescribeTableRequest, DescribeTableResponse, AttributeValue}

import scala.collection.{mutable => sm}
import scala.collection.JavaConverters._

private[spark] class DynamoPartition(idx: Int, val region: String, val table: String)
  extends Partition {
  
  override def index: Int = idx
 
}

class DynamoRdd[K: ClassTag](
    sc: SparkContext, 
    mapRow: (java.util.Map[String, AttributeValue]) => K)
  extends RDD[K](sc, Nil) {
  
  val segments = sc.conf.get("segments").toInt
  val region = sc.conf.get("region")
  val table = sc.conf.get("table")
  logInfo(s"Reading Table : $table, region : $region, segments : $segments")
  
  override def getPartitions: Array[Partition] = {
    (0 until segments).map { i =>
      new DynamoPartition(i, region, table)
    }.toArray
  }
  
  override def compute(thePart: Partition, context: TaskContext): Iterator[K] = new NextIterator[K] {
    
    val partition = thePart.asInstanceOf[DynamoPartition]
    val client = new DynamoClient(partition.region, partition.table)
    val rs = client.scanTable(partition.index, segments)
    
    override def getNext(): K = {
      if (rs.hasNext()) {
        mapRow(rs.next())
      } else {
        finished = true
        null.asInstanceOf[K]
      }
    }
    
    override def close(): Unit = {
      client.close()
    }
    
  }
}

object DynamoRdd {
  def javaMapToScala(result: java.util.Map[String, AttributeValue]): sm.Map[String, AttributeValue] = {
    result.asScala
  }
}