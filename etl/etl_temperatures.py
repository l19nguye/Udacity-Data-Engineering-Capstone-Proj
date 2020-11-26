#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, udf, when, upper
from datetime import datetime, timedelta
import pyspark.sql.types as T


# In[2]:


def create_spark_session():
    spark = SparkSession.builder                        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")                        .enableHiveSupport()                        .getOrCreate()
    return spark


# In[3]:


def process_temperature(spark, input_data, output_data):
    """
    This function will read data from CSV file into Dataframe,
    then will perform some data cleaning steps.
    
    After that, will extract data into JSON file and store in S3 bucket.
    
    Following steps will be performed:
    
        - Pyspark reading CSV file into dataframe.
        - Excluding null values from dataframe.
        - Converting columns data to correct type
        - Correcting some country names to make them consitent.
        - Selecting transformed data from columns.
        - Writing final data into JSON files in S3 bucket.
    """
        
    df = spark.read.option("header", True).csv(input_data)
    
    df = exclude_null(df)
    
    df = convert_data_col(df)
    
    df = correct_country_name(df)
    
    df_final = df.select(["Date", "AverageTemperature",                          "AverageTemperatureUncertainty",                          "City", "Country", "Latitude",                          "Longitude"])
    
    
    df_final.write.json(output_data)


# In[4]:


def correct_country_name(df):
    """
    Renaming some country names:
        - BURMA --> MYANMAR
        - CONGO (DEMOCRATIC REPUBLIC OF THE) --> CONGO
        - GUINEA BISSAU --> GUINEA-BISSAU
        - CÔTE D'IVOIRE --> IVORY COAST
    """
    
    return df.withColumn("Country",                                    when(col("Country") == "BURMA", "MYANMAR").                                    when(col("Country") == "CONGO (DEMOCRATIC REPUBLIC OF THE)", "CONGO").                                    when(col("Country") == "GUINEA BISSAU", "GUINEA-BISSAU").                                    when(col("Country") == "CÔTE D'IVOIRE", "IVORY COAST").                                    otherwise(col("Country")))


# In[5]:


def exclude_null(df):
    """
    Excluding NULL values from all columns of Dataframe.
    """
    
    return df.filter("""
                        AverageTemperature is not null
                        AND City is not null
                        AND Country is not null
                        AND Latitude is not null
                        AND Longitude is not null
                    """)


# In[6]:


def convert_data_col(df):
    """
    Convert:
        - data of column 'dt' to Date type and create new column named 'Date'.
        - data of column 'AverageTemperature' & 'AverageTemperatureUncertainty' from String to Float
        - convert all country names to uppercase string.    
    """
    
    return df.withColumn('Date', to_date(df.dt, 'yyyy-MM-dd'))            .withColumn('AverageTemperature', col("AverageTemperature").cast(T.FloatType()))            .withColumn('AverageTemperatureUncertainty', col("AverageTemperatureUncertainty").cast(T.FloatType()))            .withColumn('Country', upper(df.Country))


# In[7]:


def main():
    """
        - Create a spark session.
        - Defining path of input and output data.
        - perform ETL process by calling function process_temperature()
    """
    
    spark = create_spark_session()
    
    input_data = "../../data2/GlobalLandTemperaturesByCity.csv"
    # input_data = "s3://de-capstone/temperature/GlobalLandTemperaturesByCity.csv"
    
    output_data = "s3://de-capstone/temperature/output/"
    
    process_temperature(spark, input_data, output_data)


# In[8]:


if __name__ == "__main__":
    main()


# In[ ]:




