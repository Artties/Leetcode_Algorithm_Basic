'''
@Project ：count_year
@File    ：count_year.py
@IDE     ：PyCharm
@Author  ：Jenna C He
@Date    ：2024/3/4 18:02
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# Create a Spark session
spark = SparkSession.builder.appName("peer_id_year_count").getOrCreate()

# Sample data
data = [
    ('ABC17969(AB)', '1', 'ABC17969', 2022),
    ('ABC17969(AB)', '2', 'CDC52533', 2022),
    ('ABC17969(AB)', '3', 'DEC59161', 2023),
    ('ABC17969(AB)', '4', 'F43874', 2022),
    ('ABC17969(AB)', '5', 'MY06154', 2021),
    ('ABC17969(AB)', '6', 'MY4387', 2022),
    ('AE686(AE)', '7', 'AE686', 2023),
    ('AE686(AE)', '8', 'BH2740', 2021),
    ('AE686(AE)', '9', 'EG999', 2021),
    ('AE686(AE)', '10', 'AE0908', 2021),
    ('AE686(AE)', '11', 'QA402', 2022),
    ('AE686(AE)', '12', 'OM691', 2022)
]

# Create a DataFrame
df = spark.createDataFrame(data, ['peer_id', 'id_1', 'id_2', 'year'])

# Step 1: Get the year when peer_id contains id_2
step1_df = df.groupBy('peer_id').agg(F.min('year').alias('min_year'))

# Step 2: Count the number of each year (which is smaller or equal than the year in step1)
step2_df = df.join(step1_df, on='peer_id', how='inner') \
    .filter(F.col('year') <= F.col('min_year')) \
    .groupBy('peer_id', 'year').agg(F.count('*').alias('count'))

# Step 3: Order the value in step 2 by year and check the count number
window_spec = Window.partitionBy('peer_id').orderBy(F.desc('year'))
step3_df = step2_df.withColumn('cumulative_count', F.sum('count').over(window_spec))

final_result = step3_df.filter(F.col('cumulative_count') >= 3) \
    .orderBy('peer_id', F.desc('year')) \
    .groupBy('peer_id').agg(F.first('year').alias('result_year'))

final_result.show()