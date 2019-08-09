package org.apache.spark.datasources.dynamodb.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import java.io.IOException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import scala.collection.JavaConverters._
import org.apache.spark.datasources.dynamodb.DynamoClient
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructType, BooleanType, ArrayType}
//import org.apache.spark.sql.types.MapType
import org.apache.spark.unsafe.types.UTF8String

class DynamodbDataWriter(val table: String, val region: String, val schema: StructType) extends DataWriter[InternalRow] {

  val client = new DynamoClient(table, region)

  
  @throws(classOf[IOException])
  override def write(record: InternalRow) = {

    val itemAsMap = schema.zipWithIndex.map {
      case (field, idx) => (field.name, mapFieldType(record.get(idx, field.dataType), field.dataType))
    } collect {
      case (s, Some(i)) => (s, i.build())
    }
    client.write(itemAsMap.toMap.asJava)
  }
  
  @throws(classOf[IOException])
  override def commit(): WriterCommitMessage = {
    new DynamodbWriterCommitMessage("Task completed")
  }
  
  @throws(classOf[IOException])
  override def abort() = {
    
  }

  def mapFieldType(item: Any, field: DataType): Option[AttributeValue.Builder] = {
    val builder = AttributeValue.builder()
    if (item==null)
      return None

    field match {
      case BooleanType => Some(builder.bool(item.asInstanceOf[Boolean]))
      case ArrayType(innerType, _) => {
        val items = item.asInstanceOf[Seq[_]].map(e => mapFieldType(e, innerType)).collect({
          case Some(x) => x.build()
        })
        Some(builder.l(items:_*))
      }
      case StringType => {
        if (item.asInstanceOf[UTF8String].toString == "")
          return None
        Some(builder.s(item.asInstanceOf[UTF8String].toString))
      }
      case FloatType | DoubleType | LongType | IntegerType => Some(builder.n(item.toString))
      case MapType(keyType, valueType, _) => {

        if (keyType != StringType) throw new IllegalArgumentException(
          s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")

        val map = item.asInstanceOf[Map[String, _]].mapValues(e => mapFieldType(e, valueType)).collect({
          case (s, Some(v)) => (s, v.build())
        })
        Some(builder.m(map.asJava))
      }
    }
  }

//  def internalRowToString(row: InternalRow, idx: Int, field: StructField): String = {
//    field.dataType match {
//      case DataTypes.StringType => row.getUTF8String(idx).toString
//      case DataTypes.IntegerType => row.getInt(idx).toString
//      case DataTypes.DoubleType => row.getDouble(idx).toString
//      case DataTypes.FloatType => row.getFloat(idx).toString
//      case DataTypes.LongType => row.getLong(idx).toString
//      //case DataTypes.MapType => AttributeValue.builder().m()
//    }
//
//  }
  
}