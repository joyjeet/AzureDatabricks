# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Challenges
# MAGIC * Larger Data
# MAGIC * Faster data and decisions - seconds, minutes, hours not days or weeks after it is created
# MAGIC * Dashboards and and Reports - for the holiday season, promotional campaigns, track falling or rising trends
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Larger Data on Azure Blob Storage or Azure Data Lake
# MAGIC * Serverless Clusters
# MAGIC * Secure SQL endpoints for Spark Clusters

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Serverless SQL & BI Queries for Data at Scale 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_bi.png)

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql cache table kp_products; 
# MAGIC select * from kp_products

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from kp_products

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)[Azure Databricks & Power BI Integration Docs](https://docs.azuredatabricks.net/user-guide/bi/power-bi.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC https://docs.azuredatabricks.net/user-guide/bi/power-bi.html#connect-power-bi-desktop-to-a-databricks-cluster

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Get the JDBC server address
# MAGIC In Databricks, go to Clusters and select the cluster you want to connect.
# MAGIC 
# MAGIC On the cluster edit page, scroll down and select the JDBC/ODBC tab.
# MAGIC 
# MAGIC On the JDBC/ODBC tab, copy and save the JDBC URL.
# MAGIC 
# MAGIC Construct the JDBC server address that you will use when you set up your Spark cluster connection in Power BI Desktop.
# MAGIC 
# MAGIC Take the JDBC URL that you copied and saved in step 3 and do the following:
# MAGIC 
# MAGIC Replace jdbc:hive2 with https.
# MAGIC 
# MAGIC Remove everything in the path between the port number and sql, retaining the components indicated by the boxes in the image below.
# MAGIC 
# MAGIC In our example, the server address would be:
# MAGIC 
# MAGIC https://westus.azuredatabricks.net:443/sql/protocolv1/o/1628481937345185/0430-090710-wire545
# MAGIC 
# MAGIC Token:  dapi4f2d26db35bc6c33a5e9f11209533353
# MAGIC 
# MAGIC Step 2: Configure and make the connection in Power BI Desktop
# MAGIC Launch Power BI Desktop, click Get Data in the toolbar, and click More....
# MAGIC 
# MAGIC In the Get Data dialog, search for and select the Spark (Beta) connector.
# MAGIC 
# MAGIC Click Connect.
# MAGIC 
# MAGIC On the Spark dialog, configure your Databricks cluster connection.
# MAGIC 
# MAGIC Server: Enter the server address that you constructed from the JDBC URL in Step 1. For example, https://eastus2.azuredatabricks.net:443/sql/protocolv1/o/5441075421675247/0221-211616-tings395.
# MAGIC Protocol: Select HTTP.
# MAGIC Data Connectivity mode: Select DirectQuery, which lets you offload processing to Spark. This is ideal when you have a large volume of data or when you want near real-time analysis.
# MAGIC Click OK.
# MAGIC 
# MAGIC On the next dialog, enter the word “token” in the User name field and a personal access token in the Password field.
# MAGIC 
# MAGIC Click Connect.
# MAGIC 
# MAGIC The Power BI Navigator should display the data available for query in your Databricks cluster.

# COMMAND ----------

