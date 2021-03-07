#### Data Processing

These scripts are used to extract and transform paid parking occupancy data for the year 2012 to present and blocface to create paid parking fact, date and blockface dimension.

All of the above are done in PySpark. 

#### `databricks.py`
Standalone script to mount the data from Azure container to Databricks

#### `occupancy_transform.py`
PySpark script for transforming data stored in Azure container i.e. the Paid Parking data from '2012 to present' and Seattle Blockface data.

#### `occupancy_etl.py`
Driver PySpark script to trigger the transformation script for the above

#### `occupancy_udf.py` 
UDF to get the data records in HH:MM:SS format.



#### Set up Databricks dev env at local windows
Provision a Databricks cluster

![Alt text](Screenshot/Databricks_cluster.PNG?raw=true "DatabricksCluster")

```
Follow the instructions in the below URL and setup data bricks-connect that enables pyspark code on the local machine to be executed on Databricks cluster
* Reference: https://docs.databricks.com/dev-tools/databricks-connect.html
    * Your Spark job is planned locally but executed on the remote cluster
    * Allow the user to step through and debug Spark code in the local environment

* data bricks-connect==7.3.5 (Matching with the cluster type of 7.3.1 LTS)
* Configuration
    * The trick is one cannot mess up the delicate databricks-connect and pyspark versions
    * The Python version on local and databricks cluster should match i.e. Python 3.7.5


* Test with this example:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(spark.range(100).count())  # it executes on the cluster, where you can see the record

```



#### Execute the ETL script and trigger the transformation on the datasets via command line

```
python occupancy_etl.py

```

Final Execution Tables:

![Alt text](Screenshot/DataframeTables.PNG?raw=true "DataFrameTables")


![Alt text](Screenshot/DataframeTables_1.PNG?raw=true "DataFrameTables")



### Spark Jobs 

![Alt text](Screenshot/spark_job_ui.PNG?raw=true "SparkJobUI")
