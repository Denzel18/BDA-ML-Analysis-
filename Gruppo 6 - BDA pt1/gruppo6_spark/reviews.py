import os
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext.getOrCreate()
spark = SQLContext(sc)

customSchema = StructType([
    StructField("marketplace", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("review_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_parent", IntegerType(), True),
    StructField("product_title", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("star_rating", IntegerType(), True),
    StructField("helpful_votes", IntegerType(), True),
    StructField("total_votes", IntegerType(), True),
    StructField("vine", StringType(), True),
    StructField("verified_purchase", StringType(), True),
    StructField("review_headline", StringType(), True),
    StructField("review_body", StringType(), True),
    StructField("review_date", DateType(), True)
])
directory = '/home/amircoli/Andrea_Chiorrini/Desktop/gruppo6_spark/'
#directory = '/home/andrea/Desktop/gruppo6_spark/'
#data_complete = directory + "amazon_reviews_us_Video_Games_v1_00.tsv"
data_sample = directory + "sample_us.tsv"
test = "hdfs://192.168.104.45:9000/test.tsv"

data_reviews = test

reviews = spark.read.option("sep", "\t").csv(data_reviews, header=True, schema = customSchema)

reviews = reviews.filter(reviews.verified_purchase == "Y")\
            .select(lower(col("review_headline")).alias("review_headline"),
                    lower(col("review_body")).alias("review_body"),
                    lower(col("product_title")).alias("product_title"),
                    lower(col("star_rating")).alias("star_rating"))\
            .select(concat(
                            col("review_headline"), lit(" "), 
                            col("review_body"),
                        ).alias('review_full'),
                    col("product_title"),
                    col("star_rating"))\
            .select(regexp_replace("review_full", "[\()$#/>.]", "" ).alias("review_full"),
                    regexp_replace("product_title", "[\()$#/>.]", "" ).alias("product_title"),
                    col("star_rating"))
            
            
# Dataframe product_title
words_product_title = reviews.select(split("product_title", "\s+|,")\
                            .alias("product_title"))
words_product_title = words_product_title.select(explode(words_product_title.product_title)\
                                        .alias("words"))


# Split in parole individuali
reviews = reviews.select(split("review_full", "\s+|,").alias("review_full"),
                        col("star_rating"))

# "Explode" di tutte le parole per ogni array in review_full...
reviews = reviews.select(explode(reviews.review_full).alias("words"),
                        col("star_rating"))

reviews = reviews.withColumn('words', regexp_replace(col('words'), "\<br", ""))\
                .filter(length(col("words")) > 0)
                                
                                
# Tolgo le parole di product_title da review_full con leftanti join
reviews = reviews.join(words_product_title, ["words"], "left_anti").cache()

reviews = reviews.groupBy("words", "star_rating")\
                .count()\
                .sort('count', ascending=False)
                
#Inviduo il massimo delle occorrenze
MAX = reviews.agg(max('count')).collect()[0][0]

reviews = reviews.filter(f"count > {0.05*MAX} AND count < {0.8*MAX}")

#Sccrittura su file
star = reviews.rdd.map(lambda x : x[1]).distinct().collect()
for stars in star:
    lista_risultato = reviews.filter(f"star_rating == {stars}")\
                                .rdd.map(lambda x : (x[0], x[2]))

    url = directory + str(stars) + '.txt'
    with open(url, 'w') as filehandle:
        for riga in lista_risultato.collect():
            filehandle.write(str(riga[0]) + ';' + str(riga[1]) + "\n")
        
# Write su file con partitionBy("star_rating")
reviews.coalesce(1).write.partitionBy("star_rating")\
                    .csv(directory + "reviews_words", sep=";")

