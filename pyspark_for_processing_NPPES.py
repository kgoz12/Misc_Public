#!/usr/bin/env python
# coding: utf-8

# In[2]:


# %pip install --upgrade pip
# %pip install numpy==1.18.5
# %pip install pyspark==3.4.0
# %pip install pandas==1.3.4
# %pip install wget


# In[4]:


# %ls ./NPPES_Data_Dissemination_April_2024/


# In[5]:


import pandas as pd
import numpy as np
import os
import wget
import re

from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.functions import when

# print(platform.python_version())


# In[6]:


spark = SparkSession.builder.getOrCreate()
print(spark)
# time to upgrade to Python 3.8


# In[10]:


spark_df_csv = spark.read.option("delimiter", ",").option("header", True).option("ignoreLeadingWhiteSpace", True).option("ignoreTrailingWhiteSpace", True).option("mode", "DROPMALFORMED").csv("./NPPES_Data_Dissemination_April_2024/npidata_pfile_20050523-20240407.csv").select("NPI", 
        "`Entity Type Code`", 
        "`Provider Organization Name (Legal Business Name)`", 
        "`Provider Credential Text`",
        "`Provider First Line Business Practice Location Address`",
        "`Provider Business Practice Location Address City Name`",
        "`Provider Business Practice Location Address State Name`",
        "`Provider Business Practice Location Address Postal Code`",
        "`Provider Enumeration Date`", 
        "`NPI Deactivation Date`", 
        "`NPI Reactivation Date`", 
        "`Healthcare Provider Taxonomy Code_1`", 
        "`Healthcare Provider Primary Taxonomy Switch_1`", 
        "`Healthcare Provider Taxonomy Group_1`",        
        "`Healthcare Provider Taxonomy Code_2`", 
        "`Healthcare Provider Primary Taxonomy Switch_2`", 
        "`Healthcare Provider Taxonomy Group_2`",        
        "`Healthcare Provider Taxonomy Code_3`", 
        "`Healthcare Provider Primary Taxonomy Switch_3`", 
        "`Healthcare Provider Taxonomy Group_3`",        
        "`Healthcare Provider Taxonomy Code_4`", 
        "`Healthcare Provider Primary Taxonomy Switch_4`",
        "`Healthcare Provider Taxonomy Group_4`",
        "`Healthcare Provider Taxonomy Code_5`", 
        "`Healthcare Provider Primary Taxonomy Switch_5`",
        "`Healthcare Provider Taxonomy Group_5`",
        "`Healthcare Provider Taxonomy Code_6`", 
        "`Healthcare Provider Primary Taxonomy Switch_6`",
        "`Healthcare Provider Taxonomy Group_6`",
        "`Healthcare Provider Taxonomy Code_7`", 
        "`Healthcare Provider Primary Taxonomy Switch_7`",
        "`Healthcare Provider Taxonomy Group_7`",
        "`Healthcare Provider Taxonomy Code_8`", 
        "`Healthcare Provider Primary Taxonomy Switch_8`",
        "`Healthcare Provider Taxonomy Group_8`",
        "`Healthcare Provider Taxonomy Code_9`", 
        "`Healthcare Provider Primary Taxonomy Switch_9`",
        "`Healthcare Provider Taxonomy Group_9`",
        "`Healthcare Provider Taxonomy Code_10`", 
        "`Healthcare Provider Primary Taxonomy Switch_10`",
        "`Healthcare Provider Taxonomy Group_10`",
        "`Healthcare Provider Taxonomy Code_11`", 
        "`Healthcare Provider Primary Taxonomy Switch_11`",
        "`Healthcare Provider Taxonomy Group_11`",
        "`Healthcare Provider Taxonomy Code_12`", 
        "`Healthcare Provider Primary Taxonomy Switch_12`",
        "`Healthcare Provider Taxonomy Group_12`",
        "`Healthcare Provider Taxonomy Code_13`", 
        "`Healthcare Provider Primary Taxonomy Switch_13`",
        "`Healthcare Provider Taxonomy Group_13`",
        "`Healthcare Provider Taxonomy Code_14`", 
        "`Healthcare Provider Primary Taxonomy Switch_14`",
        "`Healthcare Provider Taxonomy Group_14`",
        "`Healthcare Provider Taxonomy Code_15`", 
        "`Healthcare Provider Primary Taxonomy Switch_15`",
        "`Healthcare Provider Taxonomy Group_15`")

fixed_names_spark_df_csv = spark_df_csv.select([F.col(column_name).alias(re.sub('[^\w]', "_", column_name)) for column_name in spark_df_csv.columns]).repartition(25)


# In[11]:


# display(spark_df_csv)
fixed_names_spark_df_csv.head(1)


# In[12]:


fixed_names_spark_df_csv.printSchema()


# In[84]:


# the text labels that go with the taxonomy codes can be viewed here:
# https://taxonomy.nucc.org
# the codes data file can be downloaded here
# https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57

taxonomy_url = "https://www.nucc.org/images/stories/CSV/nucc_taxonomy_240.csv"
taxonomy_local_path = "./nucc_taxonomy_240.csv"
wget.download(taxonomy_url, taxonomy_local_path)


# In[13]:


taxonomy = spark.read.option("delimiter", ",").option("header", True).csv("./nucc_taxonomy_240.csv").select("Code", "Grouping", "Classification", "Specialization", "Section")

taxonomy.head(1)


# In[123]:


# spark_df_csv_abbreviated = fixed_names_spark_df_csv\
# .filter(F.col("NPI").isin("1134305386"))


# In[14]:


def do_multiple_taxonomy_joins(df, n):
    if isinstance(n, int):
        join_col="Healthcare_Provider_Taxonomy_Code_"+str(n)
        grouping_name="Taxonomy_Code_Grouping_"+str(n)
        classification_name="Taxonomy_Code_Classification_"+str(n)
        specialization_name="Taxonomy_Code_Specialization_"+str(n)
        section_name="Taxonomy_Code_Section_"+str(n)
        
        new_df = df        .join(taxonomy, on = (F.col(join_col) == F.col("Code")), how="left")        .drop(F.col("Code"))        .withColumnRenamed("Grouping", grouping_name)        .withColumnRenamed("Classification", classification_name)        .withColumnRenamed("Specialization", specialization_name)        .withColumnRenamed("Section", section_name)
        return new_df
    else:
        return df


# In[15]:


# thingy = do_multiple_taxonomy_joins(df=spark_df_csv_abbreviated, n=1)
# thingy.head(1)

result_df = fixed_names_spark_df_csv

for i in range(1, 16, 1):
        result_df = do_multiple_taxonomy_joins(df=result_df, n=i)
        
result_df.head(1)


# In[17]:


# One of these taxonomy codes describes the primary function of the NPI
# Healthcare_Provider_Primary_Taxonomy_Switch_2='Y' .... means this is the primary taxonomy

with_primary_taxonomy = result_df.withColumn('Primary_Taxonomy', 
            when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_1") == 'Y', 
                 F.col("Taxonomy_Code_Specialization_1"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_2") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_2"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_3") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_3"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_4") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_4"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_5") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_5"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_6") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_6"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_7") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_7"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_8") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_8"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_9") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_9"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_10") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_10"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_11") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_11"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_12") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_12"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_13") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_13"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_14") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_14"))\
            .when(F.col("Healthcare_Provider_Primary_Taxonomy_Switch_15") == 'Y', 
                  F.col("Taxonomy_Code_Specialization_15"))\
            .otherwise('UNKNOWN'))\
.select("NPI", "Provider_First_Line_Business_Practice_Location_Address", "Primary_Taxonomy")\
.repartition(1)

with_primary_taxonomy.head(1)


# In[18]:


# write to parquet
with_primary_taxonomy.write.mode("overwrite").parquet("./parquet_out", compression='gzip')

