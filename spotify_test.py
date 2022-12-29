
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

n_song = st.selectbox(label='Please select a song', options=df1.title.unique()) 
    
query2 = ("SELECT region,count(trend) as trend FROM charts WHERE title like '{}' and trend like '%MOVE_UP%' group by region;").format(n_song)

art = spark.sql(query2).toPandas()

reg = art.region.tolist()
reg = reg[:30]
move_up = art.trend.tolist()
move_up = move_up[:30]




df1 = pd.DataFrame({
  'move_up': move_up,
  'region': reg
})

df1
st.subheader('Below graph represents the number of times the selected song has moved up in position in perticular region')
df1

if check:
    st.code('''"SELECT region,count(trend) as trend FROM charts WHERE title like '{}' and trend like '%MOVE_UP%' group by region;").format(n_song)''')
df1 = df1.rename(columns={'region':'index'}).set_index('index')    
st.bar_chart(df1)
