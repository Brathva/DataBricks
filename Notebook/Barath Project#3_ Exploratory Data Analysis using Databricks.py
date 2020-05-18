# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC #Project#3: Exploratory Data Analysis using Databricks 
# MAGIC ### CS 5165/6065: Introduction to Cloud Computing
# MAGIC 
# MAGIC ####(Total Points: 20)
# MAGIC ####Due Date: 04/23/2020 (End of the day)
# MAGIC 
# MAGIC ####Instruction:
# MAGIC ##### Perform exploratory data analysis (EDA) to gain insights using Community Edition of Databricks.
# MAGIC ### Use "Project#3: Exploratory Data Analysis using Databricks.dbc" file for the assigment
# MAGIC ##### “Databricks Overview Labs CS 5165 | 6065 v1_1.dbc” file has been provided for review
# MAGIC 
# MAGIC #### Project Tasks:
# MAGIC 
# MAGIC #####1.  Calculate the Total count of Crimes for each of the 6 United States cities listed below using 2016 crime data in dbfs:/mnt/training/crime-data-2016
# MAGIC #####2.  Provide Total count of different Types of Crimes for each of 6 United States cities listed below using crime-data-2016
# MAGIC #####3.  Calculate the total Robbery count for each of the 3 United States cities listed below using crime-data-2016
# MAGIC #####4.  Find the months with the Highest and Lowest Robbery counts for each of the 3 United States cities listed below using crime-data-2016
# MAGIC #####5.  Combine all three cities robberies-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts using crime-data-2016
# MAGIC #####6.  Plot the robberies per month for each of the three cities, producing one Graph using the contents of combinedRobberiesByMonthDF
# MAGIC #####7.  Find the "per capita robbery rates" using ther Hint below, and plot graph as above for the per capita robbery rates per month for each of the three cities, producing one Graph using the contents of robberyRatesByCityDF
# MAGIC #####8.  Find the monthly HOMICIDE counts for each of the 2 United States cities listed below using crime-data-2016, and Combine both cities HOMICIDE-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts using crime-data-2016
# MAGIC #####9. Find the "per capita HOMICIDE rates" using ther Hint in Qn 7, and plot graph as above for the per capita HOMICIDE rates per month for each of the 2 cities, producing one Graph using the contents of HOMICIDERatesByCityDF
# MAGIC #####10. A stretch goal to address a Data Science question on predicting crimes for a city as a time series model. How would you predict future values for monthly Robbery count rate for Log Angeles using the historical values from crime-data-2016.  Data Science is a multi-year discipline.  I am not expecting anything fancy.  There is no wrong answer. I do expect students to explore and give their best shot.
# MAGIC 
# MAGIC 
# MAGIC Submission:
# MAGIC 1.	Export the completed "Project#3: Exploratory Data Analysis using Databricks.dbc" as DBC Archive file with all the informatation in Blackboard
# MAGIC 2.	Your spark code/command and results included

# COMMAND ----------

# MAGIC %md
# MAGIC ### Project#3: Exploratory Data Analysis using Databricks
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a data lake.
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are a number of Parquet files containing 2016 crime data from seven United States cities:
# MAGIC 
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC 
# MAGIC The data is cleaned up a little, but has not been normalized. Each city reports crime data slightly differently, so
# MAGIC examine the data for each city to determine how to query it properly.
# MAGIC 
# MAGIC Your job is to use some of this data to gain insights about certain kinds of crimes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Project3-Setup

# COMMAND ----------

print("username: " + username)
print("userhome: " + userhome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 1:  Calculate the Total count of Crimes for each of the 6 United States cities listed below using 2016 crime data in dbfs:/mnt/training/crime-data-2016:
# MAGIC 
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC 
# MAGIC **Hint:**
# MAGIC 
# MAGIC Start by creating DataFrames for Los Angeles, Philadelphia, and Dallas data.
# MAGIC 
# MAGIC Use `spark.read.parquet` to create named DataFrames for the files you choose. 
# MAGIC 
# MAGIC To read in the parquet file, use `crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")`
# MAGIC 
# MAGIC 
# MAGIC Use the following view names:
# MAGIC 
# MAGIC | City          | DataFrame Name            | Path to DBFS file
# MAGIC | ------------- | ------------------------- | -----------------
# MAGIC | Los Angeles   | `crimeDataLosAngelesDF`   | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet`
# MAGIC | Philadelphia  | `crimeDataPhiladelphiaDF` | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet`
# MAGIC | Dallas        | `crimeDataDallasDF`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet`
# MAGIC | etc...        |

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:**  Los Angeles

# COMMAND ----------

# TODO
crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")
crimeDataLosAngelesDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet")
crimeDataPhiladelphiaDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet")
crimeDataDallasDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet")
crimeDataChicagoDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet")
crimeDataBostonDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet")
crimeDataBostonDF.createOrReplaceTempView("boston")
crimeDataLosAngelesDF.createOrReplaceTempView("losangeles")
crimeDataPhiladelphiaDF.createOrReplaceTempView("philadelphia")
crimeDataChicagoDF.createOrReplaceTempView("chicago")
crimeDataDallasDF.createOrReplaceTempView("dallas")
crimeDataNewYorkDF.createOrReplaceTempView("newyork")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select bos.rows + nyc.rows + la.rows + phil.rows + chi.rows + dal.rows 
# MAGIC           as total_crimes, bos.rows as Boston, nyc.rows as NewYork, la.rows as LosAngeles, phil.rows as Philadelphia, chi.rows as Chicago, dal.rows as Dallas
# MAGIC     from (
# MAGIC            select count(distinct(INCIDENT_NUMBER,OFFENSE_CODE)) as rows
# MAGIC            from boston
# MAGIC          ) as bos
# MAGIC                cross join (
# MAGIC             select count(distinct(complaintNumber)) as rows
# MAGIC             from newyork
# MAGIC                ) as nyc
# MAGIC                cross join (
# MAGIC             select count(distinct(id)) as rows
# MAGIC             from losangeles
# MAGIC                ) as la
# MAGIC                cross join (
# MAGIC             select count(distinct(unique_id)) as rows
# MAGIC             from philadelphia
# MAGIC                ) as phil
# MAGIC                cross join (
# MAGIC             select count(distinct(id)) as rows
# MAGIC             from chicago
# MAGIC                ) as chi
# MAGIC                cross join (
# MAGIC             select count(distinct(serviceNumberID)) as rows
# MAGIC               from dallas where lower(city) = 'dallas'
# MAGIC                ) as dal
# MAGIC  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 2: Provide Total count of different Types of Crimes for each of 6 United States cities listed below using crime-data-2016:
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC ### Notice in the Dataframes:
# MAGIC * The `crimeDataNewYorkDF` and `crimeDataBostonDF` DataFrames use different names for the columns.
# MAGIC * The data itself is formatted differently and different names are used for similar concepts.
# MAGIC 
# MAGIC This is common in a Data Lake. Often files are added to a Data Lake by different groups at different times. The advantage of this strategy is that anyone can contribute information to the Data Lake and that Data Lakes scale to store arbitrarily large and diverse data. The tradeoff for this ease in storing data is that it doesn’t have the rigid structure of a traditional relational data model, so the person querying the Data Lake will need to normalize data before extracting useful insights.
# MAGIC 
# MAGIC ## Same Type of Data, Different Structure
# MAGIC 
# MAGIC Examine crime data to determine how to extract homicide statistics.
# MAGIC 
# MAGIC Because the data sets are pooled together in a Data Lake, each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC 
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:** Los Angeles

# COMMAND ----------

# MAGIC %sql
# MAGIC (select 'BOSTON' as CITY, count(distinct(OFFENSE_CODE_GROUP)) as total_count from boston)
# MAGIC union
# MAGIC (select 'NEWYORK' as CITY, count(distinct(offenseDescription)) as total_count from newyork)
# MAGIC union
# MAGIC (select 'PHILADELPHIA' as CITY, count(distinct(text_general_code)) as total_count from philadelphia)
# MAGIC union
# MAGIC (select 'CHICAGO' as CITY, count(distinct(primaryType)) as total_count from chicago)
# MAGIC union
# MAGIC (select 'DALLAS' as CITY, count(distinct(typeOfIncident)) as total_count from dallas where lower(city) = 'dallas')
# MAGIC union
# MAGIC (select 'LOSANGELES' as CITY, count(distinct(crimeCodeDescription)) as total_count from losangeles)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 3: Calculate the total Robbery count for each of the 3 United States cities listed below using crime-data-2016:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC **Hint:**  For each table, examine the data to figure out how to extract _robbery_ statistics.
# MAGIC 
# MAGIC Each city uses different values to indicate robbery. Commonly used terminology is "larceny", "burglary" or "robbery." These challenges are common in data lakes.  To simplify things, restrict yourself to only the word "robbery" (and not attempted-roberty, larceny, or burglary).
# MAGIC 
# MAGIC Explore the data for the three cities until you understand how each city records robbery information. If you don't want to worry about upper- or lower-case, 
# MAGIC remember to use the DataFrame `lower()` method to converts column values to lowercase.
# MAGIC 
# MAGIC Create a DataFrame containing only the robbery-related rows, as shown in the table below.
# MAGIC 
# MAGIC **Hint:** For each table, focus your efforts on the column listed below.
# MAGIC 
# MAGIC Focus on the following columns for each table:
# MAGIC 
# MAGIC | DataFrame Name            | Robbery DataFrame Name  | Column
# MAGIC | ------------------------- | ----------------------- | -------------------------------
# MAGIC | `crimeDataLosAngelesDF`   | `robberyLosAngelesDF`   | `crimeCodeDescription`
# MAGIC | `crimeDataPhiladelphiaDF` | `robberyPhiladelphiaDF` | `ucr_general_description`
# MAGIC | `crimeDataDallasDF`       | `robberyDallasDF`       | `typeOfIncident`

# COMMAND ----------

# MAGIC %sql
# MAGIC select la.rows + phil.rows + dal.rows 
# MAGIC           as total_robbery, la.rows as LosAngeles, phil.rows as Philadelphia, dal.rows as Dallas
# MAGIC     from ( select count(distinct(id)) as rows
# MAGIC             from losangeles where lower(crimeCodeDescription) = 'robbery'
# MAGIC                ) as la
# MAGIC                cross join (
# MAGIC             select count(distinct(unique_id)) as rows
# MAGIC             from philadelphia where lower(ucr_general_description) = 'robbery'
# MAGIC                ) as phil
# MAGIC                cross join (
# MAGIC             select count(distinct(serviceNumberID)) as rows
# MAGIC               from dallas where lower(city) = 'dallas' and lower(typeOfIncident) like 'robbery%'
# MAGIC                ) as dal

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:** Los Angeles

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 4: Find the months with the Highest and Lowest Robbery counts for each of the 3 United States cities listed below using crime-data-2016:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC 
# MAGIC **Hint:** Now that you have DataFrames of only the robberies in each city, create DataFrames for each city summarizing the number of robberies in each month.
# MAGIC 
# MAGIC Your DataFrames must contain two columns:
# MAGIC * `month`: The month number (e.g., 1 for January, 2 for February, etc.).
# MAGIC * `robberies`: The total number of robberies in the month.
# MAGIC 
# MAGIC Use the following DataFrame names and date columns:
# MAGIC 
# MAGIC 
# MAGIC | City          | DataFrame Name     | Date Column 
# MAGIC | ------------- | ------------- | -------------
# MAGIC | Los Angeles   | `robberiesByMonthLosAngelesDF` | `timeOccurred`
# MAGIC | Philadelphia  | `robberiesByMonthPhiladelphiaDF` | `dispatch_date_time`
# MAGIC | Dallas        | `robberiesByMonthDallasDF` | `startingDateTime`
# MAGIC 
# MAGIC For each city, figure out which column contains the date of the incident. Then, extract the month from that date.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:** Los Angeles

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC (select 'LOSANGELES_MAX' as CITY, right(left(timeOccurred,7),2) as month, count(distinct(id)) as count from losangeles 
# MAGIC where lower(crimeCodeDescription) = 'robbery' group by month order by count desc limit 1 )
# MAGIC union (
# MAGIC select 'LOSANGELES_MIN' as CITY, right(left(timeOccurred,7),2) as month, count(distinct(id)) as count from losangeles 
# MAGIC where lower(crimeCodeDescription) ='robbery' group by month order by count asc limit 1 )
# MAGIC union (
# MAGIC select 'PHILADELPHIA_MAX' as CITY, right(left(dispatch_date_time,7),2) as month, count(distinct(unique_id)) as count from philadelphia 
# MAGIC where lower(ucr_general_description) = 'robbery' group by month order by count desc limit 1 )
# MAGIC union (
# MAGIC select 'PHILADELPHIA_MIN' as CITY, right(left(dispatch_date_time,7),2) as month, count(distinct(unique_id)) as count from philadelphia 
# MAGIC where lower(ucr_general_description) = 'robbery'group by month order by count asc limit 1 )
# MAGIC union (
# MAGIC select 'DALLAS_MAX' as CITY, right(left(startingDateTime,7),2) as month, count(distinct(serviceNumberID)) as count from dallas 
# MAGIC where lower(typeOfIncident) like 'robbery%' group by month order by count desc limit 1 )
# MAGIC union (
# MAGIC select 'DALLAS_MIN' as CITY, right(left(startingDateTime,7),2) as month, count(distinct(serviceNumberID)) as count from dallas 
# MAGIC where lower(typeOfIncident) like 'robbery%' group by month order by count asc limit 1 )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 5: Combine all three cities robberies-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts using crime-data-2016:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC 
# MAGIC Create another DataFrame called `combinedRobberiesByMonthDF`, that combines all three robberies-per-month views into one.
# MAGIC In creating this view, add a new column called `city`, that identifies the city associated with each row.
# MAGIC The final view will have the following columns:
# MAGIC 
# MAGIC * `city`: The name of the city associated with the row. (Use the strings "Los Angeles", "Philadelphia", and "Dallas".)
# MAGIC * `month`: The month number associated with the row.
# MAGIC * `robbery`: The number of robbery in that month (for that city).
# MAGIC 
# MAGIC **Hint:** You may want to apply the `union()` method in this example to combine the three datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:** Combined 3 Cities

# COMMAND ----------

# TODO
crimeDataLosAngelesDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet")
crimeDataPhiladelphiaDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet")
crimeDataDallasDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet")
from pyspark.sql.functions import lit
df1 = crimeDataLosAngelesDF.selectExpr("crimeCodeDescription as crime","timeOccurred as date")
df1 = df1.withColumn("city",lit("LOS ANGELES"))
df2 = crimeDataPhiladelphiaDF.selectExpr("ucr_general_description as crime","dispatch_date_time as date")
df2 = df2.withColumn("city",lit("PHILADELPHIA"))
df3 = crimeDataDallasDF.selectExpr("typeOfIncident as crime","startingDateTime as date")
df3 = df3.withColumn("city",lit("DALLAS"))
combinedRobberiesByMonthDF = df1.union(df2).union(df3)
combinedRobberiesByMonthDF.createOrReplaceTempView("combinedRobberies")



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC (select 'MAX' as Label, right(left(date,7),2) as Month, count(*) as totalRobberies from combinedRobberies
# MAGIC where (lower(crime) = 'robbery' and lower(city) != 'dallas') or (lower(crime) like 'robbery%' and lower(city) = 'dallas') group by Month order by totalRobberies desc limit 1)
# MAGIC union
# MAGIC (select 'MIN' as Label, right(left(date,7),2) as Month, count(*) as totalRobberies from combinedRobberies
# MAGIC where (lower(crime) = 'robbery' and lower(city) != 'dallas') or (lower(crime) like 'robbery%' and lower(city) = 'dallas') group by Month order by totalRobberies asc limit 1)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 6: Plot the robberies per month for each of the three cities, producing one Graph using the contents of `combinedRobberiesByMonthDF`
# MAGIC 
# MAGIC Adjust the plot options to configure the plot properly, as shown below:
# MAGIC 
# MAGIC When you first run the cell, you'll get an HTML table as the result. To configure the plot:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-4.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC 1. Click the graph button.
# MAGIC 2. If the plot doesn't look correct, click the **Plot Options** button.
# MAGIC 3. Configure the plot similar to the following example.
# MAGIC 
# MAGIC ;**Hint:** Order your results by `month`, then `city`.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-2.png" style="width: 268px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select City, right(left(date,7),2) as Month, count(*) as totalRobberies from combinedRobberies
# MAGIC where (lower(crime) = 'robbery' and lower(city) != 'dallas') or (lower(crime) like 'robbery%' and lower(city) = 'dallas') group by Month,City order by Month,City

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 7: Find the "per capita robbery rates" using ther Hint below, and plot graph as above for the per capita robbery rates per month for each of the three cities, producing one Graph using the contents of `robberyRatesByCityDF`:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC While the above graph is interesting, it's flawed: it's comparing the raw numbers of robberies, not the per capita robbery rates.
# MAGIC 
# MAGIC The DataFrame (already created) called `cityDataDF` (dbfs:/mnt/training/City-Data.parquet)  contains, among other data, estimated 2016 population values for all United States cities
# MAGIC with populations of at least 100,000. (The data is from [Wikipedia](https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population).)
# MAGIC 
# MAGIC * Use the population values in that table to normalize the robberies so they represent per-capita values (total robberies divided by population).
# MAGIC * Save your results in a DataFrame called `robberyRatesByCityDF`.
# MAGIC * The robbery rate value must be stored in a new column, `robberyRate`.
# MAGIC 
# MAGIC Next, graph the results, as above.

# COMMAND ----------

# TODO
cityDataDF = spark.read.parquet("/mnt/training/City-Data.parquet")
cityDataDF.createOrReplaceTempView("cityData")
df7 = sqlContext.sql("select City, right(left(date,7),2) as Month, count(*) as totalRobberies from combinedRobberies where (lower(crime) = 'robbery' and lower(city) != 'dallas') or (lower(crime) like 'robbery%' and lower(city) = 'dallas') group by Month,City order by Month,City")
df7.createOrReplaceTempView("df7")
robberyRatesByCityDF = sqlContext.sql("select d.City, d.Month, (d.totalRobberies/cd.estPopulation2016) as robberyRate from df7 d join cityData cd where lower(cd.city) = lower(d.City)")
display(robberyRatesByCityDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 8: Find the monthly HOMICIDE counts for each of the 2 United States cities listed below using crime-data-2016, and Combine both cities HOMICIDE-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts.  See Question 6.
# MAGIC * New York
# MAGIC * Boston
# MAGIC 
# MAGIC **Hint:**  Same Type of Data, Different Structure
# MAGIC In this section, we examine crime data to determine how to extract homicide statistics.
# MAGIC Each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.
# MAGIC 
# MAGIC To get started, create a temporary view containing only the homicide-related rows.
# MAGIC At the same time, normalize the data structure of each table so all the columns (and their values) line up with each other.
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC For the upcoming aggregation, you need to alter the New York data set to include a `month` column which can be computed from the `reportDate` column using the `month()` function. Boston already has this column.
# MAGIC 
# MAGIC In this example, we use several functions in the `pyspark.sql.functions` library, and need to import:
# MAGIC 
# MAGIC * `month()` to extract the month from `reportDate` timestamp data type.
# MAGIC * `lower()` to convert text to lowercase.
# MAGIC * `contains(mySubstr)` to indicate a string contains substring `mySubstr`.
# MAGIC 
# MAGIC Also, note we use  `|`  to indicate a logical `or` of two conditions in the `filter` method.

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, split
from pyspark.sql.functions import lit
df8 = crimeDataNewYorkDF.selectExpr("offenseDescription as crime","reportDate as date")
df8 = df8.withColumn("month1", split(col("date"), "-").getItem(1))
df8 = df8.withColumn("month", df8["month1"].cast("integer"))
df8 = df8.drop("date","month1")
df8 = df8.withColumn("city",lit("NEW YORK"))
df9 = crimeDataBostonDF.selectExpr("OFFENSE_CODE_GROUP as crime","MONTH as month")
df9 = df9.withColumn("city",lit("BOSTON"))
df10 = df8.union(df9)
df10.createOrReplaceTempView("homicideData")
bothCitiesHomicideDf = sqlContext.sql("select city, month, count(*) as totalCount from homicideData where ((lower(crime) like 'homicide%' or lower(crime) like 'murder%') and lower(city) = 'new york') or (lower(city) = 'boston' and lower(crime) = 'homicide') group by month,city order by month")
bothCitiesHomicideDf.createOrReplaceTempView("combinedHomicide")
display(bothCitiesHomicideDf)

# COMMAND ----------

# MAGIC %sql
# MAGIC (select city, 'MAX' as Label, month, totalCount from combinedHomicide order by totalCount desc limit 1)
# MAGIC union
# MAGIC (select city, 'MIN' as Label, month, totalCount from combinedHomicide order by totalCount asc limit 1)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 9: Find the "per HOMICIDE robbery rates" using ther Hint in Qn 7, and plot graph as above for the per capita HOMICIDE rates per month for each of the two cities, producing one Graph using the contents of HOMICIDERatesByCityDF:

# COMMAND ----------

HOMICIDERatesByCityDF = sqlContext.sql("select d.city, d.month, (d.totalCount/cd.estPopulation2016) as homicideRate from combinedHomicide d join cityData cd where lower(cd.city) = lower(d.city)")
display(HOMICIDERatesByCityDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 10: A stretch goal to address a Data Science question on predicting crimes for a city as a time series model. How would you predict future values for monthly Robbery Count rate for Log Angeles using the historical values from crime-data-2016.  Data Science is a multi-year discipline.  I am not expecting anything fancy.  There is no wrong answer. I do expect students to explore and give their best shot.

# COMMAND ----------

display(crimeDataLosAngelesDF)

# COMMAND ----------

from pyspark.sql.functions import col, lower
from pyspark.sql.functions import month, col, count
import numpy as np
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
robberiesByMonthLosAngeles = (crimeDataLosAngelesDF.select(month(col("timeOccurred")).alias("month"),lower(col("crimeCodeDescription")).alias("crime"))
  .filter("crimeCodeDescription = 'ROBBERY'")
  .orderBy("month")
  .groupBy("month")
  .agg(count(col('crime')).alias("robberies"))
)
expression = [col(column).alias(column.replace(' ', '_')) for column in robberiesByMonthLosAngeles.columns]
robberiesByMonthLosAngeles = robberiesByMonthLosAngeles.select(*expression).selectExpr("month as month", "robberies as label")
stages = []
Vec_assembler = VectorAssembler(inputCols=["month"], outputCol="features")
stages += [Vec_assembler]
pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(robberiesByMonthLosAngeles)
dataset = pipelineModel.transform(robberiesByMonthLosAngeles)
selectedcols = ["features", "label"]
xaxis = dataset.rdd.map(lambda p: (p.features[0])).collect()
yaxis = dataset.rdd.map(lambda p: (p.label)).collect()
plt.style.use('classic')
plt.rcParams['lines.linewidth'] = 0
plot, axis = plt.subplots()
axis.loglog(xaxis,yaxis)
plt.xlim(0.0e5, 1.0e7)
plt.ylim(0.0e1, 1.0e3)
axis.scatter(x, y, c="blue")
from pyspark.ml.regression import LinearRegression
lin_reg = LinearRegression()
# to fit the dataset
import numpy as np
from pandas import *
mod1 = lin_reg.fit(dataset, {lin_reg.regParam:0.0})
mod2 = lin_reg.fit(dataset, {lin_reg.regParam:100.0})
mod1_pred = mod1.transform(dataset)
mod2_pred = mod2.transform(dataset)
month = dataset.rdd.map(lambda p: (p.features[0])).collect()
robberies = dataset.rdd.map(lambda p: (p.label)).collect()
prediction_1 = mod1_pred.select("prediction").rdd.map(lambda r: r[0]).collect()
prediction_2 = mod2_pred.select("prediction").rdd.map(lambda r: r[0]).collect()
dataframe_python = DataFrame({'month':month,'robberies':robberies,'predicted_1':prediction_1, 'predicted_2':prediction_2})

# COMMAND ----------

display(dataframe_python)

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC 
# MAGIC The crime data used in this notebook comes from the following locations:
# MAGIC 
# MAGIC | City          | Original Data 
# MAGIC | ------------- | -------------
# MAGIC | Boston        | <a href="https://data.boston.gov/group/public-safety" target="_blank">https&#58;//data.boston.gov/group/public-safety</a>
# MAGIC | Chicago       | <a href="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2" target="_blank">https&#58;//data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2</a>
# MAGIC | Dallas        | <a href="https://www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data" target="_blank">https&#58;//www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data</a>
# MAGIC | Los Angeles   | <a href="https://data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq" target="_blank">https&#58;//data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq</a>
# MAGIC | New Orleans   | <a href="https://data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data" target="_blank">https&#58;//data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data</a>
# MAGIC | New York      | <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i" target="_blank">https&#58;//data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i</a>
# MAGIC | Philadelphia  | <a href="https://www.opendataphilly.org/dataset/crime-incidents" target="_blank">https&#58;//www.opendataphilly.org/dataset/crime-incidents</a>
