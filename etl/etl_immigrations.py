#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import to_date, col, udf
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.types as T


# In[2]:


def create_spark_session():
    spark = SparkSession.builder                    .config("spark.jars.packages",                            "saurfang:spark-sas7bdat:3.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2")                    .enableHiveSupport()                    .getOrCreate()
    return spark


# In[3]:


def process_immigration(spark, input_data, output_data):
    """
    This function will read data from SAS file into Dataframe,
    then will perform some data cleaning steps.
    
    After that, will extract data into parquet files and store in S3 bucket.
    
    Following steps will be performed:
    
        - Pyspark reading SAS files into dataframe.
        - Converting columns data to correct type
        - Excluding null values from dataframe.
        - Writing final data into parquet files in S3 bucket.
    """
        
    df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    # df = spark.read.parquet(input_data)
    
    df = convert_data_col(df)
    
    df = exclude_invalid(df)    
    
    df.write.json(output_data)


# In[4]:


def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(float(x)))
    except:
        return None


# In[5]:


udf_convert_to_datetime = udf(lambda x: convert_datetime(x), T.DateType())


# In[6]:


def exclude_invalid(df):
    """
    Excluding NULL values from all columns of Dataframe.
    And only select records with valid state code
    """
    valid_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 
                    'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 
                    'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 
                    'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 
                    'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 
                    'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 
                    'WV', 'WI', 'WY' ]
    
    return df.filter(col("year").isNotNull() & 
                    col("month").isNotNull() &
                    col("fromCountry").isNotNull() &
                    col("destCountry").isNotNull() &
                    col("age").isNotNull() & 
                    col("birth_year").isNotNull() & 
                    col("admission_number").isNotNull() & 
                    col("arrive_date").isNotNull() & 
                    col("departure_date").isNotNull() &
                    col("gender").isNotNull() &
                    col("flight_number").isNotNull() &
                    col("reside_in").isNotNull() &
                    col("airline").isNotNull() &
                    col("transportation_mode").isNotNull() &
                    col("reside_in").isin(valid_states) == True)


# In[7]:


def convert_data_col(df):
    """
        - convert columns 'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 
        'i94bir', 'biryear', 'i94mode' to Integer type.
        - convert columns 'admnum' to Long Type
        - convert columns 'arrdate', 'depdate' to Date Type
        - also naming columns with meaningful string.
    """
    
    return df.withColumn("id", col("cicid").cast(T.IntegerType()))             .withColumn("year", col("i94yr").cast(T.IntegerType()))             .withColumn("month", col("i94mon").cast(T.IntegerType()))             .withColumn("fromCountry", col("i94cit").cast(T.IntegerType()))             .withColumn("destCountry", col("i94res").cast(T.IntegerType()))             .withColumn("age", col("i94bir").cast(T.IntegerType()))             .withColumn("birth_year", col("biryear").cast(T.IntegerType()))             .withColumn("admission_number", col("admnum").cast(T.LongType()))             .withColumn("arrive_date", udf_convert_to_datetime(col("arrdate")))             .withColumn("departure_date", udf_convert_to_datetime(col("depdate")))             .withColumn("transportation_mode", col("i94mode").cast(T.IntegerType()))             .selectExpr(["id", "year", "month", "fromCountry", "destCountry", "gender",                         "age", "birth_year", "admission_number", "arrive_date", "departure_date",                         "fltno as flight_number", "airline", "i94addr as reside_in", "transportation_mode"])


# In[8]:


def main():
    """
        - Create a spark session.
        - Defining path of input and output data.
        - perform ETL process by calling function process_airports()
    """
    
    spark = create_spark_session()
    
    input_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    #input_data = "s3://de-capstone/immigration/input/"
    
    output_data = "s3://de-capstone/immigration/output/"
    
    process_immigration(spark, input_data, output_data)


# In[9]:


if __name__ == "__main__":
    main()


# In[ ]:




