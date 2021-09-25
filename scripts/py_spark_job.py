from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date, monotonically_increasing_id, split
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, LongType

import sys, os
import logging

from utils import load_yml_config

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

logging.info("Loading configuration files")
config = load_yml_config(os.path.join("./", "config", "etl.yml"))

PATH_OUT = config["data"]['output_bucket']

IMMIGRATION_KEY = config["data"]['immigration_data_key']
AIRPORT_KEY = config['data']['airport_data_key']
DEMOGRAPHICS_KEY = config['data']['demographics_data_key']
TEMPERATURES_KEY = config["data"]['temperatures_data_key']

os.environ['jdk.xml.entityExpansionLimit'] = '0' 
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.4 pyspark-shell'

def prepare_demographic_data():
    """
    Python function responsible to prepare the demographic data
    """
    
    demographic_schema = StructType([
        StructField('City', StringType()),
        StructField('State', StringType()),
        StructField('Median Age', DoubleType()),
        StructField('Male Population', IntegerType()),
        StructField('Female Population', IntegerType()),
        StructField('Total Population', IntegerType()),
        StructField('Number of Veterans', IntegerType()),
        StructField('Foreign-born', IntegerType()),
        StructField('Average Household Size', DoubleType()),
        StructField('State Code', StringType()),
        StructField('Race', StringType()),
        StructField('Count', IntegerType()),
    ])

    demographic_data = DEMOGRAPHICS_KEY
    df_demog = spark.read.format('csv').options(header=True, delimiter=";", ).schema(demographic_schema).load(demographic_data)
    
    logging.info("Number of rows in original demographic dataset: {}".format(df_demog.count()))
    
    # Make sure rows are unique 
    df_demog_uni = df_demog.dropDuplicates(["Race", "City", "State"])
    df_demog_renamed = df_demog_uni.withColumnRenamed('City', 'city')\
        .withColumnRenamed('State', 'state')\
        .withColumnRenamed('Median Age', 'median_age')\
        .withColumnRenamed('Male Population', 'male_population')\
        .withColumnRenamed('Female Population', 'female_population')\
        .withColumnRenamed('Total Population', 'total_population')\
        .withColumnRenamed('Number of Veterans', 'number_of_veterans')\
        .withColumnRenamed('Foreign-born', 'number_of_foreign_born')\
        .withColumnRenamed('Average Household Size', 'average_household_size')\
        .withColumnRenamed('State Code', 'state_code')\
        .withColumnRenamed('Race', 'race')\
        .withColumnRenamed('Count', 'count')\
        .withColumn('id_demog', monotonically_increasing_id())\
        .withColumn('country', lit('United States'))
    
    logging.info("Number of rows in demographic dataset after drop duplicates: {}".format(df_demog_renamed.count()))
    
    file_name = demographic_data.split("/")[-1].split('.')[0]
    df_demog_renamed.write.mode('overwrite').parquet(f"{PATH_OUT}/dim_{file_name}.parquet")
    

def prepare_airport_data():
    """
    Python function responsible to prepare the airport data
    """

    airport_schema = StructType([
        StructField('ident', StringType()),
        StructField('type', StringType()),
        StructField('name', StringType()),
        StructField('elevation_ft', IntegerType()),
        StructField('continent', StringType()),
        StructField('iso_country', StringType()),
        StructField('iso_region', StringType()),
        StructField('municipality', StringType()),
        StructField('iata_code', StringType()),
        StructField('coordinates', StringType())
    ])
    
    airport_data = AIRPORT_KEY
    df_airport = spark.read.format('csv').options(header=True, delimiter=",").schema(airport_schema).load(airport_data)
    
    df_airport_uni = df_airport.dropDuplicates(["ident"])
    df_airport_new_columns = df_airport_uni \
        .withColumn('latitude', split(df_airport_uni['coordinates'], ',').getItem(0)) \
        .withColumn('longitude', split(df_airport_uni['coordinates'], ',').getItem(1)) \
        .withColumn('country', lit('United States'))
    df_dropped = df_airport_new_columns.drop('coordinates')
    
    file_name = airport_data.split("/")[-1].split('.')[0]
    df_dropped.write.mode('overwrite').parquet(f"{PATH_OUT}/dim_{file_name}.parquet")

def prepare_immigration_data():
    """
    Python function responsible to prepare the immigration data
    """
    immigration_data = IMMIGRATION_KEY
    df_imi = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data)
    
    df_imi_uni = df_imi.dropDuplicates(["cicid"])
    df_imi_new = df_imi_uni.withColumn('country', lit('United States'))
    
    file_name = immigration_data.split("/")[-2]
    df_imi_new.write.mode('overwrite').parquet(f"{PATH_OUT}/fact_{file_name}.parquet")

def prepare_temperatures_data():
    """
    Python function responsible to prepare the temperatures data
    """

    temperature_schema = StructType([
        StructField('dt', DateType()),
        StructField('AverageTemperature', DoubleType()),
        StructField('AverageTemperatureUncertainty', DoubleType()),
        StructField('City', StringType()),
        StructField('Latitude', StringType()),
        StructField('Longitude', StringType())
    ])

    temperatures_data = TEMPERATURES_KEY
    df_temperatures = spark.read.format('csv').options(header=True, delimiter=",").schema(temperature_schema).load(temperatures_data)
    
    df_temperatures_uni = df_temperatures.dropDuplicates(["dt", 'City', 'Country'])
    df_temperatures_renamed = df_temperatures_uni.withColumnRenamed('AverageTemperature', 'average_temperature')\
        .withColumnRenamed('AverageTemperatureUncertainty', 'average_temperature_uncertainty')\
        .withColumnRenamed('City', 'city')\
        .withColumnRenamed('Country', 'country')\
        .withColumnRenamed('Latitude', 'latitude')\
        .withColumnRenamed('Longitude', 'longitude')\
        .withColumn('id_temp', monotonically_increasing_id())

    file_name = temperatures_data.split("/")[-1].split('.')[0]
    df_temperatures_renamed.write.mode('overwrite').parquet(f"{PATH_OUT}/dim_{file_name}.parquet")

def create_spark_session():
    """
    This function creates a configured PySpark Session to work with Spark in Python
    
    Returns:
        SparkSession: Return a Spark Session object able to work throughout the code.
    """
    return SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").getOrCreate()


if __name__ == "__main__":
    spark = create_spark_session()
    
    prepare_demographic_data()
    prepare_airport_data()
    prepare_immigration_data()
    prepare_temperatures_data()

