
#### Data Processing

#### Set up Databricks dev env at local windows
```
Follow the instructions in the below URL and setip databricks-connect that enables pyspark code on local machine to be executed on Databricks cluster
* Reference: https://docs.databricks.com/dev-tools/databricks-connect.html
    * Your Spark job is planned in local but executed on the remote cluster
    * Allow the user to step through and debug Spark code in the local environment

* databricks-connect==7.3.5 (Matching with the cluster type of 7.3.1 LTS)
* Configuration
    * The trick is one cannot mess up the delicate databricks-connect and pyspark versions
    * The Python version on local and databricks cluster should match i.e. Python 3.7.5

* Test with this example:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(spark.range(100).count())  # it executes on the cluster, where you can see the record

```

#### Execute the ETL script and trigger the transformation on the datasets

```
python occupancy_etl.py

```

### Spark Jobs 
