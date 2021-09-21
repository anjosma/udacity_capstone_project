#%%
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id

import sys, os
sys.path.insert(0, '/'.join( os.getcwd().split("/")[:-1] ))
from plugins.utils import load_yml_config

import logging

logging.info("Loading configuration files")
credentials = load_yml_config(os.path.join("./", "config", "credentials.yml"))
config = load_yml_config(os.path.join("./", "config", "etl.yml"))

S3_BUCKET_IN = config["aws"]['s3']['input_bucket']
S3_BUCKET_OUT = config["aws"]['s3']['output_bucket']

IMMIGRATION_KEY = config["aws"]['s3']['immigration_data_key']
AIRPORT_KEY = config["aws"]['s3']['airport_data_key']
DEMOGRAPHICS_KEY = config["aws"]['s3']['demographics_data_key']

logging.info("Setting AWS credentials")

ACCESS_KEY = credentials['aws']["access_key"]
SECRET_KEY = credentials['aws']["secret_key"]

os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
os.environ["AWS_SECRET_KEY_ID"] = SECRET_KEY

os.environ['jdk.xml.entityExpansionLimit'] = '0' 
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.4 pyspark-shell'

#%%
def prepare_demographic_data():
#%%
    demographic_data = os.path.join(S3_BUCKET_IN, DEMOGRAPHICS_KEY)
    df = spark.read.format('csv').options(header=True, delimiter=";").load(demographic_data)
    df.show()

def prepare_airport_data():
#%%
    airport_data = os.path.join(S3_BUCKET_IN, AIRPORT_KEY)
    df = spark.read.format('csv').options(header=True, delimiter=",").load(airport_data)
    df.show()

#%%
def create_spark_session():
    """
    This function creates a configured PySpark Session to work with Spark in Python
    
    Returns:
        SparkSession: Return a Spark Session object able to work throughout the code.
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("fs.s3a.access.key", ACCESS_KEY) \
        .config("fs.s3a.secret.key", SECRET_KEY) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


if __name__ == "__main__":
    spark = create_spark_session()
    #prepare_demographic_data()
    prepare_airport_data()

# %%
