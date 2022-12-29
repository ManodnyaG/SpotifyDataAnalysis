
import streamlit as st
st.title('Spotify Data Explorer')
st.text('This is a web app to allow exploration of Spotify Charts Data')

import pandas as pd 
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import seaborn as sns

import matplotlib.pyplot as plt

add_selectbox = st.sidebar.selectbox(
    "What would you like to look at first",
    ("Big data analysis", "Songs","About")
)

#df1 = pd.read_csv('/Users/csuftitan/Downloads/charts.csv')
df1 = pd.read_csv('test.csv')
df1.head()
st.header('Header of Dataframe')
st.write(df1.head())

spark = SparkSession.builder.appName("spark_app").getOrCreate()

#df = spark.read.csv(path='/Users/csuftitan/Downloads/charts.csv', inferSchema=True, header=True)
df = spark.read.csv('test.csv', inferSchema=True, header=True)


df = df.withColumn("rank", f.col("rank").cast(t.LongType())).withColumn("date", f.col("date").cast(t.DateType())).withColumn("streams", f.col("streams").cast(t.IntegerType()))
df = df.na.drop()
df.count()
df.registerTempTable("charts")

n_country = st.selectbox(label='Please select a country', options=df1.region.unique()) 
    
query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = '{}' GROUP BY title  ORDER BY count DESC;".format(n_country) 

reg = spark.sql(query).toPandas().head(10) 
st.subheader("Based on the country selected, below graph represents which song has been at rank 1 most number of times")
check = st.checkbox('View query')
if check:
    st.code('query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = {} GROUP BY title  ORDER BY count DESC;".format(n_country)')
fig = plt.bar(reg['title'],reg['count'])
plt.xticks(rotation=90)
plt.title('Title Vs Count')
plt.xlabel('title')
plt.ylabel('count')
st.pyplot()
st.set_option('deprecation.showPyplotGlobalUse', False)
