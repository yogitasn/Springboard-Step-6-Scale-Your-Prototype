#### Ingestion
The Seattle Parking Occupancy Dataset are csv files provided as Year to Date data from 2012 to present

Pre-requistes: 
[Details](0 - setup\setup.md) Instructions for setting up the Azure environment before starting the ingestion process

occupancy_ingest.py
[Details] Python script for downloading the files from Seattle Open Data to Azure file share

post_ingestion.docx
[Details] Instructions for moving the data from file share to Azure storage to read them in Pyspark dataframe in the data processing step