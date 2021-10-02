# Capstone Project Udacity Data Engineering
This is the last project of six from Udacity Nanodegree Data Engineering Program.

# Scope
This project aims to prepare datasets for ELT (Extract, Load and Transform) in a Datalake using PySpark and Pandas to ensure data quality for all the information processed in the datasets. For this, we will use Jupyter Notebook to explore the datasets, PySpark to process high volume of data. The scripts written on this project has as the main goal creates a analytical visualization for the datasets to make data available for a Data Science Team for studies cases and to make it easier for Business Teams to create their own Business Intelligence Visualization.
With the data available here we can answare questions about immigration in USA, like: the most popular cities, races in each state, gender distribution, median age, birthyear of new immigration, etc. Otherwise we can also understand the relation of high temperatures with the arrive of latin america in United States.

# Datasets
The following datasets will be used on this project:
- I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
- World Temperature Data: This dataset came from Kaggle and has a lot of information about temperature during period of a time.
- U.S. City Demographic Data: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.
- Airport Code Table: It contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

## Airport Data
It contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

### Dictionary
- ident: Unique key for each row
- type: Type of depart
- name: Name of Depart place
- elevation_ft:
- continent: Name of the depart's continent
- iso_country: Code of the country
- iso_region: Code of the region (COUNTRY-STATE)
- municipality: Name of the municipality depart
- gps_code: Gps Code
- iata_code: Iata Code
- local_code: Local depart code
- coordinates: Latitude and longitude of the depart place

## Demographic Data
This data comes from OpenSoft. This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey.

### Dictionary
- City: City name
- State: USA State
- Median Age: The median age from the city's population
- Male Population: Number of Male people in the city
- Female Population: Number of Female people in the city
- Number of Veterans: Number of Veterans people in the city
- Foreign-born: Number of people that were born in other cities
- Average Household Size: Average number of people that lives in a household
- State Code: Code of USA State
- Race: People's race
- Count: Number of people by race.


## Temperatures Data
This dataset came from Kaggle and has a lot of information about temperature during period of a time.

### Dictionary
- dt: Date reference
- AverageTemperature: Global average land temperature in celsius
- AverageTemperatureUncertainty: The 95% confidence interval around the average
- City: Name of the city
- Country: Name of the country
- latitude: Latitude position
- longitude: Longitude position


## Immigration Data
This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

# Explore and Access Data
These steps are included in `notebook/Capstone Project.ipynb`.

# Model data
The model data used here is the Star Schema. It was choose because we have a 'main' table: Immigration and we also have assistants tables to support us and to create a relationship with that table, as: Airport, Temperatures and Demographics data. The datas are denormalized, because we want the data in a analytics form, to make it easier to another teams to use the data. If we use that data normalized, it could be tough to other teams to make their own queries and understand the tables.
The Star Schema also make it easier to write new queries, for better perfomances and to use it and achieve business insights.

# Run the code
The process is composed by two scripts:
- `py_spark_job.py`: Execute the ELT process, gathering the data from all datasets, transforming and loading into `parquet` files.
- `data_quality_checks.py`: Check if the output of the previously process were expected.

# Project Write Up
## What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
The goal of this model is to make an ELT model to make the RAW data available for not only data science but also business teams. With ELT model, the teams are free to develop their own rules with data, making the teams less dependents of data engineering teams.  
Spark in EMR could be used for a huge volume of data and to make it available on cloud and Airflow can be used to schedule the process.

## Propose how often the data should be updated and why.
According to the data available on these datasets, some tables can be updated daily and others monthly. The update interval must be set up according to the necessity of the team.

## Include a description of how you would approach the problem differently under the following scenarios:
- **If the data was increased by 100x**: We can use AWS EMR to use parallel processing and make it possible to execute a huge volume of data.
- **If the pipelines were run on a daily basis by 7am**: We can use Apache Airflow. With that we can schedule a lot of DAGs to make it available for a company. Also, in a company with ELT process, we can democratize the SQL and make the business people able to develop their only queries to be scheduled in Airflow.
- **If the database needed to be accessed by 100+ people**: AWS Redshift fits well on this case. It's a cloud column partitioned database for analytics purposes.