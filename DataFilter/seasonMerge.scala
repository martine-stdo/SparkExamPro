import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("TitleWordCount").getOrCreate()

val firstSeasonDF = spark.read.option("header", true).csv("hdfs://hadoop01:9000/firstSeason.csv")
val secondSeasonDF = spark.read.option("header", true).csv("hdfs://hadoop01:9000/secondSeason.csv")
val thirdSeasonDF = spark.read.option("header", true).csv("hdfs://hadoop01:9000/thirdSeason.csv")

val firstSeasonWordCount = firstSeasonDF.groupBy("Title").count()
val secondSeasonWordCount = secondSeasonDF.groupBy("Title").count()
val thirdSeasonWordCount = thirdSeasonDF.groupBy("Title").count()

firstSeasonWordCount.coalesce(1).write.csv("hdfs://hadoop01:9000/firstSeasonWordcount.csv")
secondSeasonWordCount.coalesce(1).write.csv("hdfs://hadoop01:9000/secondSeasonWordcount.csv")
thirdSeasonWordCount.coalesce(1).write.csv("hdfs://hadoop01:9000/thirdSeasonWordcount.csv")
spark.stop()
