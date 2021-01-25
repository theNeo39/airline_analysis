# **Airline Dataset Analysis using Hive**

## **Project Overview**
We have a dataset consists of flight arrival and departure details for all commercial flights within the USA, from October 1987 to April 2008.
This is a large dataset: there are nearly 120 million records in total, and takes up 1.6 gigabytes of space compressed(bzip2) and 12 gigabytes when uncompressed(csv).
The goal of this project is to summarize the data by time periods, carriers,airports and understand different factors causing the airline delays.

## **Dataset**
This dataset can be find here [Airline on-time performance](http://stat-computing.org/dataexpo/2009/).
1. 1987-2009 (.bz2 format)
2. airports.csv
3. carriers.csv
4. plane-data.csv

## **Business Logic**
1. Are some time periods more prone to delays than others.
2. What are the best hours to travel.
3. Are some airports more prone to delays than others.
4. Are some carriers more prone to delays than others.
5. Do old planes suffer more delays.

## **Prerequisite Concepts**
1. Good understanding of Hadoop Environment.
2. Hive with Partitioning and Bucketing.
3. Tez/mapreduce framework.
4. Dimensional modeling for OLAP.
5. Splittable and non-splittable compression formats.
6. Different file formats(Avro, ORC, Parquet).
7. Data Driven Development for project building.

## **Datawarehouse Schema**

**Star Schema** is considered for data modeling because we are focused on analysing the data ie.. more read operations.

#### **Fact Table**

**data_fact_table** - records all the commercial flights arrival and departure details with delays.

#### **Dimension Table**

**airports_dim_table** - contains information about the airports.  
**carriers_dim_table** - contains information about the carriers.  
**plane_dim_table** - contains information about the planes.  

## **Project Development**

#### **Step 1**

We have the historical data, for batch processing let us ingest the dataset into
our on-prem Hadoop HDFS.

* Connecting to hortonworks Hadoop docker using SSH  
``` ssh root@127.0.0.1 -p 2222```
* copy the entire data using SCP to docker from host machine.  
```scp -P 2222 /input_data root@127.0.0.1:/input_data```

* copy data from local file system to HDFS.  
```hdfs dfs -copyFromLocal /input_data/* /input_data```

#### **Step 2**

* Created staged table to understand the data types and transformations required to batch processing.  
* Created static partitioning on year for fast retrieval for text format.  

#### **Step 3**

* Created final dimensional tables and fact table with appropriate data types and necessary transformation using hive UDF functions.
* Created Dynamic partitioning on year for fast retrieval for ORC format.
* This process has columnar format storage for best compression storage.

#### **Step 4**

* Adhoc queries for getting the answers for the business logic.

**Note** If lets say we get the data every year or month then we can ingest data by creating a staged table
and insert the delta data into final table using ```insert into..```

## **Analysis Results**

* We see that **April, May and September** has minimum delays than other months and are best
time to fly whereas **June and December** has maximum delays.
* We see that morning flights between **4-5 AM** are best to fly whereas **6-8 PM** are worst to fly.  
* We see that **Chicago, Atlanta,Dallas** being the most delayed planes when compared to others.
* We see that **Hawaiian ,Aloha** arilines are better to fly.
* We **dont see year as factor** for being prone to more delays.

**Note** we can perform many more adhoc query analysis based on the requirement.

## **How To Run**

1. Run the ```main.py``` to get everything setup.
    * use tableau to visualize the requirement and necessary analysis using views.
    * use HQL for necessary analysis.  

