import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val spark = SparkSession.builder().appName("rank").getOrCreate()
val inputPath = "hdfs://hadoop01:9000/comment3.csv"
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(inputPath)
val titleCounts = df.groupBy("Title").count()
val ouputPath = "hdfs://hadoop01:9000/rank2.csv"
titleCounts.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(ouputPath)