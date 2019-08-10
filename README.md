# spark-dynamodb

Experimenting with a Spark custom data source for DynamoDB.

Uses DataSourceV2 APIs for compatibility with Spark 2.4

## Examples

### Reading
```scala
val spark = SparkSession
      .builder()
      .appName("Test Read DynamoDataSource")
      .getOrCreate()

val dynamoDf = spark.read.format("org.apache.spark.datasources.dynamodb.DynamodbDataSource")
  .option("dynamodb.read.segments", 100)
  .option("dynamodb.capacity", 2000)
  .option("dynamodb.table.name", "my-test-table")
  .option("dynamodb.table.region", "us-west-2")
  .load()
```

### Writing
```scala
val spark = SparkSession
      .builder()
      .appName("Test Read DynamoDataSource")
      .getOrCreate()

dynamoDf.write
      .format("org.apache.spark.datasources.dynamodb.DynamodbDataSource")
      .option("dynamodb.capacity", 2000)
      .option("dynamodb.table.name", "my-test-table")
      .option("dynamodb.table.region", "us-west-2").save()
```
