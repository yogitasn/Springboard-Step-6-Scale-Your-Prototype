from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType,DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.functions import col,array_contains,date_format,regexp_replace
import logging
import configparser
from pathlib import Path

schema = StructType() \
                .add("OccupancyDateTime",StringType(),True) \
                .add("Occupied_Spots",IntegerType(),True) \
                .add("BlockfaceName",StringType(),True) \
                .add("SideOfStreet",StringType(),True) \
                .add("Station_Id",StringType(),True) \
                .add("ParkingTimeLimitCategory",IntegerType(),True) \
                .add("Available_Spots",IntegerType(),True) \
                .add("PaidParkingArea",StringType(),True) \
                .add("PaidParkingSubArea",StringType(),True) \
                .add("PaidParkingRate",DoubleType(),True) \
                .add("ParkingCategory",StringType(),True) \
                .add("Location",StringType(),True)


spark = SparkSession\
        .builder\
        .getOrCreate()

       
occ_df_2018_2020 = spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load(['dbfs:/mnt/FileStore/MountFolder/2018_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2019_Paid_Parking.csv'])

occ_df_2018_2020.show(3,truncate=False)
#display(get_dbutils(spark).fs.ls("dbfs:/mnt/FileStore/MountFolder"))
