package org.apache.spark.datasources.dynamodb.reader

import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import java.io.IOException
import org.apache.spark.datasources.dynamodb.{DynamoClient, DynamodbDataSourceOptions}
import software.amazon.awssdk.services.dynamodb.model.{ScanResponse, AttributeValue}
import org.apache.spark.internal.Logging
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import scala.collection.mutable.HashSet

class DynamodbInputPartitionReader(table: String, region: String,
                                   capacity: Int,
                                   segment: Int,
                                   totalSegments: Int,
                                   schema: StructType) extends InputPartitionReader[InternalRow] with Logging {
  
  inputPartitionReader =>
    
  @transient lazy val listener = new ExecutorAllocationListener(capacity, totalSegments)

  
  val ddbClient = new DynamoClient(table, region)
  var lastEvaluatedKey: java.util.Map[String, AttributeValue] = _
  var lastResultBlock: java.util.Iterator[java.util.Map[String, AttributeValue]] = _
  var rateLimiter: RateLimiter = _
  var lastConsumedCapacity: Int = _
  initReader()
  
  @throws(classOf[IOException])
  def next(): Boolean = {
    lastResultBlock.hasNext() || (lastEvaluatedKey != null)
  }
  
  private def asInternalRow(item: java.util.Map[String, AttributeValue]): InternalRow = {
    
//    val stringSchema = schema.map(field => {
//      val name, dataType = (field.name, field.dataType)
//      s"$name:$dataType"
//    }).mkString(";")
//    logInfo(s"schema found : $stringSchema")
    
    val values = schema.map(f => DynamoClient.fromAttributeValue(f.dataType, Option(item.get(f.name))))
    InternalRow.fromSeq(values)
  }
  
  private def initReader() = {
    val scanResult = ddbClient.scan(segment, totalSegments)
    val resultCount = scanResult.count()
    logInfo(s"First Scan, Page size : $resultCount") 
    lastEvaluatedKey = scanResult.lastEvaluatedKey()
    lastResultBlock = scanResult.items().iterator()
    
    lastConsumedCapacity = scanResult.consumedCapacity().capacityUnits().toInt
    logInfo(s"First Scan, Consumed capacity : $lastConsumedCapacity") 
    
    val rateLimit = capacity/totalSegments
    log.info(s"Segment $segment using rate limit of $rateLimit")
    rateLimiter = RateLimiter.create(rateLimit)
  }
  
  def get(): InternalRow = {
    
    
    if (lastResultBlock.hasNext())
      return asInternalRow(lastResultBlock.next())
    
    else if (lastEvaluatedKey != null) {
      rateLimiter.acquire(lastConsumedCapacity)
      val scanResult = ddbClient.scan(segment, totalSegments, lastEvaluatedKey)
      val resultCount = scanResult.count()
      logInfo(s"Next Scan, Page size : $resultCount") 
      lastEvaluatedKey = scanResult.lastEvaluatedKey()
      lastResultBlock = scanResult.items().iterator()
      lastConsumedCapacity = scanResult.consumedCapacity().capacityUnits().toInt
      logInfo(s"Resetting Consumed capacity : $lastConsumedCapacity")
      return asInternalRow(lastResultBlock.next())
    }
    
    else
      InternalRow()

  }
  
  def close() = {
    
  }
  
  class ExecutorAllocationListener(capacity: Int, segments: Int) extends SparkListener with Logging {
    
    private val executorIds = new HashSet[String]
    
    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (!executorIds.contains(executorId))
        executorIds.add(executorId)
        
      val executorCount = executorIds.size
      logInfo(s"New executor added, now using $executorCount total executors")
      
      
      val maxParallelSegments = math.min(segments, executorCount)
      val rateLimit = capacity/maxParallelSegments
      logInfo(s"Resetting segment $segment to use rate limit of $rateLimit")
    
      inputPartitionReader.synchronized {
        inputPartitionReader.rateLimiter = RateLimiter.create(rateLimit)
      }
//      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
//        // This guards against the race condition in which the `SparkListenerTaskStart`
//        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
//        // possible because these events are posted in different threads. (see SPARK-4951)
//        if (!allocationManager.executorIds.contains(executorId)) {
//          allocationManager.onExecutorAdded(executorId)
//        }
//      }
    }
  
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    
      val executorId = executorRemoved.executorId
      if (executorIds.contains(executorId))
        executorIds.remove(executorId)
        
      val executorCount = executorIds.size
      logInfo(s"Executor removed, now using $executorCount total executors")
      
      val maxParallelSegments = math.min(segments, executorCount)
      val rateLimit = capacity/maxParallelSegments
      logInfo(s"Resetting segment $segment to use rate limit of $rateLimit")
      
      //allocationManager.onExecutorRemoved(executorRemoved.executorId)
      inputPartitionReader.synchronized {
        inputPartitionReader.rateLimiter = RateLimiter.create(rateLimit)
      }
    }
}
  
}

