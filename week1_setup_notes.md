## Task 1: Azure Databricks Environment Setup & Familiarization
### Cluster Provisioning
Cluster name:  my-first-cluster
Policy: Unrestricted
Databricks runtime: 16.4 LTS (includes Apache Spark 3.5.2, Scala 2.12)
Node Type: Standard_D4s_v3
Terminate after 30 minutes
Access mode: [Legacy] Single user standard


No challenge at this stage.


## What was the most challenging part? 
 Configuring the spark cluster was not a challenging but , creating a database from the given csv file was challenge still not resolved, I just found another way of creating the table. The issue is, since I am using the free tier in azure databricks and with that it could not allow me to use unity catalog and a result, could not create the table with the UI at the specified/expected path. As a work around, I have just used another location to store the table.

## What was the most interesting discovery? 
The fact that databricks let's you change from python to sql with the same notebook is was interesting

## What questions do you still have
how I can update the config for enabling the unity catalog with my current subscription and therey by check if the issue I have encountered is trully a limitation of free tier.