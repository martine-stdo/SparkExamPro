import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// 创建SparkSession
val spark = SparkSession.builder().appName("DataGroupingAndSorting").getOrCreate()

// 读取CSV文件
val df = spark.read.option("header", "true").csv("hdfs://hadoop01:9000/comment1.csv")

// 将Date列转换为日期类型
val dfWithDate = df.withColumn("Date", to_date(col("Date"), "MMM d, yyyy"))

// 根据年和月进行分组排序
val sortedDF = dfWithDate.orderBy(col("Date")).select("Sno", "Title", "Date", "User", "text").coalesce(1) // 将结果合并为单个分区，生成单个CSV文件

// 生成结果文件
sortedDF.write.format("csv").option("header", "true").mode("overwrite").save("hdfs://hadoop01:9000/sorted_comments.csv")
