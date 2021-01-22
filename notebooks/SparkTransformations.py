# Databricks notebook source
# DBTITLE 1,withColumnRenamed
# Creating a DataFrame
from pyspark.sql.types import IntegerType # To Define our dataframe

df = spark.createDataFrame([1, 2], IntegerType()).toDF("id") # Giving the field name as "id"

df.show()

# How can we RENAME a COLUMN ? -  withColumnRenamed method. 

newDF = df.withColumnRenamed("id", "renamedID" ) 
newDF.show()

# COMMAND ----------

# DBTITLE 1,Casting to Integer
# Creating a DataFrame
from pyspark.sql.types import StringType # To Define our dataframe

from pyspark.sql.functions import col # Returns a Column based on the given column name.

df = spark.createDataFrame([1, 2], StringType()).toDF("id") # Giving the field name as "id"

df.show()
df.printSchema()

# How can we change the datatype of a column? -  withColumn method. ( Casting to Integer )

newCastedDF = df.withColumn("id", col("id").cast("Integer")) 
newCastedDF.printSchema()

# COMMAND ----------

# DBTITLE 1,WITHCOLUMN
df = spark.createDataFrame([1, 2], IntegerType()).toDF("id") # Giving the field name as "id"

# How can we change the value of the existing column?

value_changed_df = df.withColumn("id", col("id") * 2) # Multiplying the value with 2

print("------------------Print 2 Records of Value Changed DF-----------")
value_changed_df.show(2)

# How can we derived new column from an existing?

new_derived_column_df = df.withColumn("new_id", col("id") * 4) # Creating a New Column (new_id) from existing column (id)
print("------------------Print 2 Records of New DF (With 2 Columns Now) -----------")
new_derived_column_df.show(2)

# COMMAND ----------

# DBTITLE 1,UNION
from pyspark.sql.types import * # Importing for creating custom User Defined Schema

my_schema = StructType([        # Creating schema of 2 columns - id , name
  StructField("id", IntegerType(), True),
  StructField("name", StringType(), True)
])

my_list = ['deepak', 'leena', 'ankit', 'madhu'] # My List

data = [[0, 'deepak'], [1, 'leena']]

df = spark.createDataFrame(enumerate(my_list), schema=my_schema) # Id should be generated - Used enumerate

null_df = spark.createDataFrame([[None, 'priya'], [5, None]], schema=my_schema) # Generating 2 records with Nulls

null_df.show() # In first record id is NULL , In second record name is NULL

appended_df = df.union(null_df) # Appending one Dataframe with Another DF

appended_df.show() # All 6 records will be displayed

# COMMAND ----------

# DBTITLE 1,FILTER
appended_df.show() # We have the dataframe of previous example

from pyspark.sql.functions import col

missing_name_df = appended_df.filter(col("name").isNull()) # Retrieve only rows with missing name

missing_name_df.show()

print("-------------------Filtering Null Values with isNull() method of col function-------------")

missing_id_df = appended_df.filter(col("id").isNull()) # Retrieve only rows with missing id
missing_id_df.show()

# COMMAND ----------

# DBTITLE 1,COUNT & countDistinct
from pyspark.sql.functions import countDistinct

csvDF = spark.read.option("inferSchema", True).option("header", True).csv("/databricks-datasets/flights/departuredelays.csv")# Create DF via csv File
print("------------------Printing DF---------------")
csvDF.show(2)

totalDestDf = csvDF.groupBy("origin").agg({'destination': 'count'}) # We want to count the total number of destinations from origin
totalDestDf.show(2)

# We want to count unique destination from a particular origin
uniqueDestDf= csvDF.groupBy("origin").agg(countDistinct("destination").alias("uniquedest"))
uniqueDestDf.show(2)

# COMMAND ----------

# DBTITLE 1,FILTER vs WHERE
parquetDF = spark.read.parquet("/mnt/training/City-Data.parquet") # Create DF via Parquet File

print("------------------Printing first 1 records of Parquet DF---------------")
parquetDF.show(1)

filteredDF = parquetDF.filter(col("state") == "New York") # filter() records from state = New York
print("------------------Printing first 1 records of Filtered DF---------------\n")
filteredDF.show(1)

filter_by_where_df = parquetDF.where((col("state") == "California") & (col("city") == "Los Angeles"))

print("------------------Printing first 1 records of Filtered DF by where()---------------\n")
filter_by_where_df.show() # The where() clause is equivalent to filter()

# COMMAND ----------

# DBTITLE 1,ORDERYBY SORTBY
# orderBy() and sort()

parquetDF = spark.read.parquet("/mnt/training/City-Data.parquet") # Create DF via Parquet File
print("------------Demo: sort()---------------------") # asc() and desc() function can be used accordingly
parquetDF.sort("state", "city").show(4)


print("------------Demo: orderBy()---------------------")
parquetDF.orderBy("state", "city", ascending = 0).show(4) # ascending = 1 means descending

# COMMAND ----------

# DBTITLE 1,GROUPBY ORDERBY
parquetDF = spark.read.parquet("/mnt/training/City-Data.parquet") # Create DF via Parquet File
print("------------------Printing first 5 records of Parquet DF---------------")
parquetDF.show(5)

# Apply GroupBy and Agg Operations and Orderby - state desc

groupedDF = (parquetDF
             .groupBy("state")
             .agg({"city": "count"})
             .withColumnRenamed("count(city)", "TotalCityCounts")
             .orderBy("state", ascending = 0)
            ) # option 0 for descending

print("------------------Printing first 5 records of Grouped / Aggregated / ordered DF---------------")
groupedDF.show(5)

# COMMAND ----------

# DBTITLE 1,AGG on MULTIPLE COLUMNS
parquetDF = spark.read.parquet("/mnt/training/City-Data.parquet") # Create DF via Parquet FiLE
# # groupBy and aggregate on single column
parquetDF.groupBy("state").min("population2010").orderBy("state", ascending = 1).show(4)

# groupBy and aggregate on multiple columns
from pyspark.sql.functions import min, max, avg, sum, mean, count

(parquetDF.groupBy("state").agg(min("population2010").alias("min_population")\
                              ,max("population2010").alias("max_population")\
                              ,sum("estPopulation2016").alias("est_population")\
                              ,count("city").alias("total_no_cities")\
                              ,mean("estPopulation2016").alias("mean_estPopulation2016"))\
                              .orderBy("state", ascending = 1).show(5)) # ascending True

# COMMAND ----------

# DBTITLE 1,DISTINCT vs dropDuplicates
df = spark.range(4) # Simplest way of creating a dataframe ( 4 records DF )

from pyspark.sql.functions import lit # lit() : Creates a Column of literal value

# Creating Duplicate Records Dataframe
duplicate_records_df = df.withColumn("id", lit(1905)).withColumn("name", lit("deepak"))
print("-------------------Printing Duplicate Records DF--------------------")
duplicate_records_df.show()

distinct_df = duplicate_records_df.distinct() # Will give distinct rows
print("-------------------Printing Distinct Records DF--------------------")
distinct_df.show()

dropping_dup_df = duplicate_records_df.dropDuplicates() # Will give distinct rows ( Optionally can supply subset of columns)
print("-------------------Dropping Duplicate Records DF--------------------")
dropping_dup_df.show() # Results will be like above in this case

# COMMAND ----------

# DBTITLE 1,LIT, WITHCOLUMNRENAMED, DROP
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit # Creates a Column of literal value
df = spark.createDataFrame([1, 2, 3, 4], IntegerType()).toDF("id") # Giving the field name as "id"

# How can we add a new column with a constant value?
new_added_column_df = df.withColumn("name", lit("Deepak")) # Creating a column - "name" with value "Deepak"
new_added_column_df.show(2) # Printing 2 Rows

# Rename a Column
rename_column_df = new_added_column_df.withColumnRenamed("name", "emp_name") # Renaming column from "name" to "emp_name"
rename_column_df.show(2) # Printing 2 Rows 

# Drop a Column
drop_column_df = rename_column_df.drop("emp_name") # Dropping a Column "emp_name"

# COMMAND ----------

# DBTITLE 1,REPARTITON vs COALESCE
print(parquetDF) # parquet Dataframe
# getNumPartitions() method is the RDD method so ".rdd"
print("Number of Partitions in the Source Dataframe", str(parquetDF.rdd.getNumPartitions())) 

# Repartioning into 4 partitions ( know your data source / number of Cores ) 1024 MB / 128 MB - 8 - df == 8 but Core ( 16 ) - only 
repartitionedDF = parquetDF.repartition(4) # Increase / decrease -- Shuffle 8 --> 16 --> 10 
 
print("Number of Partitions in the New Partitioed Dataframe", str(repartitionedDF.rdd.getNumPartitions()))

# It will always reduce the partitions 
coalescedDF = repartitionedDF.coalesce(2) # Decrease 1024 --> parquet ( parts ) --> 8 partitons : 4 6 8 - write file / table (  )16 / 8 .write
 
print("Number of Partitions in the New Coalesced Dataframe", str(coalescedDF.rdd.getNumPartitions()))

# COMMAND ----------

# DBTITLE 1,Splitting Columns
from pyspark.sql import Row
from pyspark.sql.functions import col, split

df_row = Row("id", "name", "city")

row1 = df_row(1, "deepak,kumar,rajak", "T-Hyderabad")
row2 = df_row(2, "virendra,singh,solanki", "MP-Bhoapl")
row3 = df_row(3, "sarvesh,kumar,mishra", "MH-Pune")
row4 = df_row(4, "vasavi,,devi", "AP-Vizag")
row5 = df_row(5, "leena,,rajak", "CG-Bilaspur")
df_row_seq = [row1, row2, row3, row4, row5]

df = spark.createDataFrame(df_row_seq) # Creating a Dataframe
df.show(2)

splitting_name_df = df.withColumn("firstname", split(col("name"), ",")[0]).\
                       withColumn("middlename", split(col("name"), ",")[1]).\
                       withColumn("lastname", split(col("name"), ",")[2]).drop(col("name")) # dropping the original column
splitting_name_df.show()

# COMMAND ----------

# DBTITLE 1,Imputing Null or Missing Data
# Creating a dataframe
corruptDF = spark.createDataFrame([ (11, "Deepak", 5), \
                                   (12, "Leena", None), \
                                   (13, "Deepa", 7), \
                                   (14, None, 9), \
                                   (15, "Preety", None)], \
                                  ["id", "name", "bday"])
# Handling NULLs
drop_nulls = corruptDF.na.drop() # Dropping NULL Values ( Only 2 Rows will be left)

drop_nulls.show()

fill_nulls_with_value = corruptDF.na.fill({"name" : "Unknown", "bday": 1}) # ( Filling NULL Values)
fill_nulls_with_value.show() # name we are filing as "Unknown", bday we are filling as "1"