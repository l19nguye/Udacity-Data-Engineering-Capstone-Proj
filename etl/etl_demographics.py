#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pyspark.sql.types as T


# In[2]:


def create_spark_session():
    spark = SparkSession.builder                    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")                    .enableHiveSupport()                    .getOrCreate()
    return spark


# In[3]:


def process_demographics(spark, input_data, output_data):
    """
    This function will read data from CSV file into Dataframe,
    then will perform some data cleaning steps.
    
    After that, will extract data into JSON file and store in S3 bucket.
    
    Following steps will be performed:
    
        - Pyspark reading CSV file into dataframe.
        - Converting columns data to correct type
        - Correcting some country names to make them consitent.
        - Excluding null values from dataframe.        
        - Selecting transformed data from columns.
        - Writing final data into JSON files in S3 bucket.
    """
        
    df = spark.read.option("header", True).options(delimiter=";").csv(input_data)
    
    df = convert_data_col(df)
    
    df = exclude_null(df)   
    
    df_final = df.selectExpr(["City", "State", "MedianAge", "MalePopulation", 
                                "FemalePopulation", "TotalPopulation", "NumberVeterans", 
                                "ForeignBorn", "AverageHouseholdSize", "Race", "Count"])
    
    
    df_final.write.json(output_data)


# In[4]:


def exclude_null(df):
    """
    Excluding NULL values from all columns of Dataframe.
    """
    
    return df.filter("""
                        MedianAge is not null AND MedianAge > 0
                        AND MalePopulation is not null AND MalePopulation > 0
                        AND FemalePopulation is not null AND FemalePopulation > 0
                        AND TotalPopulation is not null AND TotalPopulation > 0
                        AND NumberVeterans is not null AND NumberVeterans > 0
                        AND ForeignBorn is not null AND ForeignBorn > 0
                        AND AverageHouseholdSize is not null AND AverageHouseholdSize > 0
                        AND Race is not null
                        AND Count is not null AND Count > 0
                      """)


# In[5]:


def convert_data_col(df):
    """
        - from columns 'Median Age', 'Average Household Size' to Float type.
        - from columns 'Male Population', 'Female Population', 'Total Population',
        'Number of Veterans', 'ForeignBorn', 'Count' to Integer type.
    """
    
    return df.withColumn("MedianAge", col("Median Age").cast(T.FloatType()))                .withColumn("MalePopulation", col("Male Population").cast(T.IntegerType()))                .withColumn("FemalePopulation", col("Female Population").cast(T.IntegerType()))                .withColumn("TotalPopulation", col("Total Population").cast(T.IntegerType()))                .withColumn("NumberVeterans", col("Number of Veterans").cast(T.IntegerType()))                .withColumn("ForeignBorn", col("Foreign-born").cast(T.IntegerType()))                .withColumn("AverageHouseholdSize", col("Average Household Size").cast(T.FloatType()))                .withColumn("Count", col("Count").cast(T.IntegerType()))


# In[6]:


def main():
    """
        - Create a spark session.
        - Defining path of input and output data.
        - perform ETL process by calling function process_airports()
    """
    
    spark = create_spark_session()
    
    # input_data = 's3://de-capstone/demographics/us-cities-demographics.csv'
    input_data = 'us-cities-demographics.csv'
    
    output_data = "s3://de-capstone/demographics/output/"
    
    process_demographics(spark, input_data, output_data)


# In[7]:


if __name__ == "__main__":
    main()


# In[ ]:




