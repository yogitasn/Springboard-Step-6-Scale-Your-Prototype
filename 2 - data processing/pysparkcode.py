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

       
occ_df_2018_2020 = spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load(['dbfs:/mnt/FileStore/MountFolder/2018_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2019_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2020_Paid_Parking.csv'])

occ_df_2012_2017 = spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load(['dbfs:/mnt/FileStore/MountFolder/2012_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2013_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2014_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2015_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2016_Paid_Parking.csv',\
                   'dbfs:/mnt/FileStore/MountFolder/2017_Paid_Parking.csv'])

occ_df=occ_df_2018_2020.withColumn('Station_Id',regexp_replace(col('Station_Id'),'[\'\s,]',''))
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


occ_df = occ_df.withColumn("Station_Id",\
                occ_df["Station_Id"].cast(IntegerType()))

occ_df = occ_df.withColumn("OccupancyDateTime", \
                       F.to_timestamp(occ_df.OccupancyDateTime, format="mm/dd/yyyy hh:mm:ss a"))
        
occ_df = occ_df.drop("BlockfaceName",
           "SideOfStreet",
           "ParkingTimeLimitCategory",
           "PaidParkingArea",
           "PaidParkingSubArea",
           "PaidParkingRate",
           "ParkingCategory")


occ_df=occ_df.withColumn('Longitude',F.split('Location',' ').getItem(1)) \
             .withColumn('Latitude',F.split('Location',' ').getItem(2))

#  occ_df=occ_df.withColumn('Latitude',parkingOccpn_udf.braceRepl('Latitude')) \
#              .withColumn('Longitude',parkingOccpn_udf.braceRepl('Longitude'))

occ_df=occ_df.withColumn('Latitude',regexp_replace(col('Latitude'), "\(|\)", "")) \
             .withColumn('Longitude',regexp_replace(col('Longitude'), "\(|\)", ""))


occ_df = occ_df.withColumn("Latitude",occ_df["Latitude"].cast(DoubleType())) \
               .withColumn("Longitude",occ_df["Longitude"].cast(DoubleType()))


occ_df=occ_df.drop('Location')

occ_df_2020 = spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load('dbfs:/mnt/FileStore/MountFolder/2020_Paid_Parking.csv')


import pyspark.sql.functions as F

station_id_lookup=occ_df_2020.select('Station_Id','Longitude','Latitude').distinct()

occ_df_2012_2017 = occ_df_2012_2017\
                            .join(station_id_lookup, ['Station_Id'], how='left_outer')\
                            .select(occ_df_2012_2017.OccupancyDateTime,occ_df_2012_2017.Station_Id,\
                                   occ_df_2012_2017.Occupied_Spots,occ_df_2012_2017.Available_Spots,\
                                   station_id_lookup.Latitude,station_id_lookup.Longitude)


from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import re

# udf to remove ' and , in the column value
# regex_replace
#commaRep = udf(lambda x: re.sub('[\'\s,]','', x))

# udf to remove '(' and ')' in the column value
#braceRepl = udf(lambda x: re.sub('\(|\)','', x))


def format_minstoHHMMSS(x):
    """
    Function to convert the minutes to HH:MM:SS format
    """
    try:
        duration=timedelta(minutes=int(x))
        seconds = duration.total_seconds()
        minutes = seconds // 60
        hours = minutes // 60
        return "%02d:%02d:%02d" % (hours, minutes % 60, seconds % 60)
    except:
        None

# udf for entire dataframe
udf_format_minstoHHMMSS=udf(lambda x: format_minstoHHMMSS(x))