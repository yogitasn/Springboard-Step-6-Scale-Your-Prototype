import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from occupancy_transform import ParkingOccupancyLoadTransform


def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.\
            appName("Transforming the historical parking occupancy and blockface datasets").\
            getOrCreate()


def main():
    """
    This method performs below task:
   
    Transform paid parking and blockface data present in working zone
    
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    pot = ParkingOccupancyLoadTransform(spark)

    # Modules in the project
    modules = {
        "parkingOccupancy.csv": pot.transform_load_parking_hist_2018_2020_occupancy,
        "blockface.csv" : pot.transform_load_blockface_dataset,
        "parkingOccupancy_arch":pot.transform_load_parking_hist_2012_2017_occupancy
    }

    for file in modules.keys():
        modules[file]()

# Entry point for the pipeline
if __name__ == "__main__":
    main()