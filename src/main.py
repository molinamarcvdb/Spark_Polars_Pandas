# %%

# %% [markdown]
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

app_name = "NY_Taxi_Data"
csv_path = "data/yellow_tripdata_2019-01.csv"
# Initialize Spark session
spark = SparkSession.builder.appName(app_name).getOrCreate()

# Read the CSV file
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Display the schema
df.printSchema()

# Show the first few rows
df.show(5)

# SHow csv columns
columns = df.columns

print('The dataset has the follwing columns:', columns)

# Describe the dataset
df.describe().show()

# Descibre only some columns
columns_to_describe = ["trip_distance", "payment_type", "total_amount"]

df.select(columns_to_describe).describe().show()

## Perform some basic analysis like selecting the avg trip distance and the maximum amount payed per ride
df.select(avg("trip_distance"), max("total_amount")).show()

## Group by passenger count and calculate average fare
df.groupBy("passenger_count").agg(avg("fare_amount").alias("avg_fare")).orderBy("passenger_count").show()

## Use collect() this bring s the distibuted data to the driver side as the local data in python this can crash if ww do not have enough memory to store all the data iun the driver side
#from py4j.protocol import Py4JJavaError
#
#try:
#    df.collect()
#except Py4JJavaError as e:
#    print(e)
#    df.take(2) # AVoid throwing the memeory error 
#    # Returns the first num rows as a list of Row.
## OR
#    df.tail(2)
#    # Returns the last num rows as a list of Row.
# Why would we need collect:
# This can be useful for performing further analysis, visualization, or integration with non-Spark libraries or tools.
# However thsi has many downsides like: Memory overhead, performance impact, data loss, limited parallelism
# Alternatives to collect:
#Sampling: Instead of collecting all the data, you can use the sample method to extract a representative subset for analysis.
#Writing to External Storage : If you need to persist or save the data for further analysis, consider writing it to an external storage system like HDFS, Amazon S3, or a database.
#**Using foreach or mapPartitions: Instead of collecting data to the driver, you can apply custom logic to each partition using foreach or mapPartitions. This way, you can process data in parallel on worker nodes without bringing it to the driver.Alternatives to collect:
#Sampling: Instead of collecting all the data, you can use the sample method to extract a representative subset for analysis.
#Writing to External Storage : If you need to persist or save the data for further analysis, consider writing it to an external storage system like HDFS, Amazon S3, or a database.
#**Using foreach or mapPartitions: Instead of collecting data to the driver, you can apply custom logic to each partition using foreach or mapPartitions. This way, you can process data in parallel on worker nodes without bringing it to the driver.
 
## Access a single column:  is lazily evaluated and simply selecting a column does not trigger the computation 
df.trip_distance # THis restrun a column instance 

## THis happens with many column-wise operations all return the same column type:
from pyspark.sql import Column
from pyspark.sql.functions import upper

print(type(df.trip_distance) == type(upper(df.congestion_surcharge)) == type(df.congestion_surcharge.isNull()))

print(type(df.trip_distance))

## Create a new column, witha  codnition from another column info

# Calculate the mean fare amount
mean_fare = df.select(mean('fare_amount')).first()[0]

df = df.withColumn('aboveMeanFare', 
                             when(col('fare_amount') > mean_fare, True)
                             .otherwise(False))
## Print only the newly create column
df.select("aboveMeanFare").show()

## Print only the rows
df.filter(df.aboveMeanFare==True).count()


## Group by vendor and check the avg values of each numerical column
df.groupBy('VendorID').avg().show()

## We can groupBy and then apply a fucntion in pandas inc ase it s way easeir too implementn or it is not possible in spark although thsi mgiht nnot be best practice
from pyspark.sql.types import DoubleType, StructField
from pyspark.sql.functions import lit

def price_p_passenger(pandas_df):
    return pandas_df.assign(priceppass=pandas_df.total_amount / pandas_df.passenger_count)

df = df.withColumn('priceppass', lit(0.0))
# In thsi way we are actually only applying that fucntiuoin per group, lets say we want to sum or divide by the average fo that group, the problem comes when creating a new variable as the apply in pandas will expect this new column to be already present
#df.groupBy('VendorID').applyInPandas(price_p_passenger, schema=df.schema).show()


## Another thing we can do is to cogroup for this we will load another dataset froma ntoher month also with the same column
second_csv_path = './data/yellow_tripdata_2019-02.csv'

df2 = spark.read.csv(second_csv_path, header=True, inferSchema=True)#header=True, inferSchema=True) cogroup returns a different type of object than groupBy alone.

import pandas as pd

def combine_avg_fares(key, left_iter, right_iter):
    # If key is a tuple, take the first element, otherwise use it as is
    vendor_id = key[0] if isinstance(key, tuple) else key
    
    left_df = pd.DataFrame(left_iter, columns=['fare_amount'])
    right_df = pd.DataFrame(right_iter, columns=['fare_amount'])
    
    left_avg = left_df['fare_amount'].mean() if not left_df.empty else None
    right_avg = right_df['fare_amount'].mean() if not right_df.empty else None
    
    return pd.DataFrame({
        'VendorID': [int(vendor_id)],  # Use vendor_id instead of key
        'left_avg_fare': [float(left_avg) if left_avg is not None else None],
        'right_avg_fare': [float(right_avg) if right_avg is not None else None]
    })

result = df.groupBy('VendorID').cogroup(
    df2.groupBy('VendorID')
).applyInPandas(
    combine_avg_fares,
    "VendorID int, left_avg_fare double, right_avg_fare double"
)

# Calculate overall average and order results
final_result = result.withColumn(
    'overall_avg_fare',
    (when(col('left_avg_fare').isNull(), 0).otherwise(col('left_avg_fare')) +
     when(col('right_avg_fare').isNull(), 0).otherwise(col('right_avg_fare').cast('double'))) / 2
).orderBy('overall_avg_fare')

final_result.show()


## Now lets use some SQL queries
## Lets start registering the table in sql
df.createOrReplaceTempView("TableA")

## Let's count the rows in table A
spark.sql("SELECT   count(*) from TableA").show()

##UDFs can be registered and invoked in SQL out of the box:
@pandas_udf("integer")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

spark.udf.register("add_one", add_one)
spark.sql("SELECT add_one(VendorID) FROM tableA").show()


## MLib
