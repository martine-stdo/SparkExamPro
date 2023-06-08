import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("CSV Processing").getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://hadoop01:9000/comment3.csv")
import org.apache.spark.sql.functions._
val dfWithDate = df.withColumn("Date", to_date(col("Date"), "MMM d, yyyy"))
val firstSeasonStart = to_date(lit("Jan 1"), "MMM d")
val firstSeasonEnd = to_date(lit("Mar 31"), "MMM d")
val secondSeasonStart = to_date(lit("Apr 1"), "MMM d")
val secondSeasonEnd = to_date(lit("Jul 31"), "MMM d")
val thirdSeasonStart = to_date(lit("Aug 1"), "MMM d")
val thirdSeasonEnd = to_date(lit("Dec 31"), "MMM d")

dfWithDate.filter((month(col("Date")) >= 1 && month(col("Date")) <= 3) || (year(col("Date")) > 2023)).coalesce(1).write.csv("hdfs://hadoop01:9000/firstSeason.csv")
dfWithDate.filter(month(col("Date")) >= 4 && month(col("Date")) <= 7).coalesce(1).write.csv("hdfs://hadoop01:9000/secondSeason.csv")
dfWithDate.filter(month(col("Date")) >= 8 && month(col("Date")) <= 12).coalesce(1).write.csv("hdfs://hadoop01:9000/thirdSeason.csv")
