## Table of contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Execution](#execution)
* [Screenshot](#screenshot)

## General Info
This project is scaling the prototype of Open Ended Data Engineering Project: Seattle Paid Parking Occupancy

<hr/>

## Overview

Parking issues have been receiving increasing attention. An accurate parking occupancy prediction is considered to be a key prerequisite to optimally manage limited parking resources. However, parking prediction research that focuses on estimating the occupancy for various parking lots, which is critical to the coordination management of multiple parks (e.g., district-scale or city-scale), is relatively limited.

This project is to scale the data pipeline prototyped in Step Five to work with the entire (Big) dataset.

A pipeline is built on cloud using Python, Pyspark and cloud technologies like Azure Storage, Azure VM, Azure DataFactory and Azure Databricks 

* Extraction: File extraction process is automated using Selenium Python library and headless Chrome driver.
* Transformation: After files are extracted, transformations are performed using Pyspark (Python API to support Spark)

<hr/>


## Technologies
Project is created with:
* Azure Storage-Containers/File Share: To store the big data
* Azure Virtual Machine: To execute the ingestion script to download the data to file share
* Azure Data Factory: To execute a pipeline to copy data from file share to Azure containers
* Databricks-connect: Allow the user to step through and debug Spark code in the local environment and execute it on remote databricks cluster
    * Python 3.7.5 (which matches the remote cluster python version)
    [Reference](https://docs.databricks.com/dev-tools/databricks-connect.html)


## Execution

Navigate to project folder and execute the following commands

* Extraction (Script to download occupancy data files to a file share path : 'Z:\<fileshare>\'

```
python occupancy_ingest.py

```

* Transformation and loading by executing the below python driver file.  Driver will call the transformation code for performing transformations on data 2018-2020 and 2012-2017 due to varying column data formats and Blockface data

```
python occupancy_etl.py

```