from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, count, desc, row_number
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Step 1: Read XML Files
xml_files_path = "/app/xml_files/*.xml"
df = spark.read.format("xml").options(rowTag="document").load(xml_files_path)

# Step 2: Data Cleansing
required_tags = ["doc-number", "invention-title", "abstract", "description"]
df_cleaned = df.select([col(tag.lower()) for tag in required_tags])


# Step 3: Data Quality Validation
df_validated = df_cleaned.filter(col("invention-title").isNotNull() & col("abstract").rlike("English"))

# Step 4: Calculation 1 - Most frequent words in description
stop_words = StopWordsRemover().getStopWords()  # English stop words
df_tokens = df_validated.withColumn("tokens", split(lower(col("description")), "\\W+"))
df_filtered_tokens = df_tokens.select("doc-number", "description", explode("tokens").alias("token")) \
    .filter(~col("token").isin(stop_words))
df_word_count = df_filtered_tokens.groupBy("doc-number", "description", "token").agg(count("*").alias("count"))
df_most_frequent_words = df_word_count.withColumn("rn", row_number().over(Window.partitionBy("doc-number")
                                                                  .orderBy(desc("count"))))
df_most_frequent_words = df_most_frequent_words.filter(col("rn") <= 2).drop("rn")

# Step 5: Calculation 2 - Top 1000 most frequent words
df_global_word_count = df_filtered_tokens.groupBy("token").agg(count("*").alias("num_occurrences"))
df_top_1000_words = df_global_word_count.orderBy(desc("num_occurrences")).limit(1000)
df_top_1000_words = df_top_1000_words.withColumn("timestamp", current_timestamp())

# # Step 6: Persist Dataframes as Delta tables
# patents_schema = "patents"
# df_most_frequent_words# 
# Step 6: Print the DataFrames
print("Most Frequent Words:")
df_most_frequent_words.show()

print("Top 1000 Words:")
df_top_1000_words.show()
