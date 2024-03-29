import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover}
import org.apache.spark.sql._

// 读取comment.csv
val inputPath = "hdfs://hadoop01:9000/comment3.csv"
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(inputPath)

// Tokenize文本
val tokenizer = new Tokenizer().setInputCol("Title").setOutputCol("words")
val tokenized = tokenizer.transform(df)

// 删除停止词
val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords")
val filtered = remover.transform(tokenized)

// 计算词频
val wordCounts = filtered.select(explode(col("filteredWords")).as("word")).groupBy("word").count().orderBy(desc("count"))

// 写入wordcount.csv
// 将数据集的分区数减少到1
// 改成输出目录，而不是具体的文件名
val outputPath = "hdfs://hadoop01:9000/Animationwordcount_output" 
wordCounts.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(outputPath)