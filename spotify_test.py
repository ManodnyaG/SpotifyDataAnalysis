{
  "metadata": {
    "language_info": {
      "codemirror_mode": {
        "name": "python",
        "version": 3
      },
      "file_extension": ".py",
      "mimetype": "text/x-python",
      "name": "python",
      "nbconvert_exporter": "python",
      "pygments_lexer": "ipython3",
      "version": "3.8"
    },
    "kernelspec": {
      "name": "python",
      "display_name": "Python (Pyodide)",
      "language": "python"
    }
  },
  "nbformat_minor": 4,
  "nbformat": 4,
  "cells": [
    {
      "cell_type": "code",
      "source": "import pandas as pd \nimport numpy as np",
      "metadata": {
        "trusted": true
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": "df1 = pd.read_csv('test.csv')",
      "metadata": {
        "trusted": true
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": "import streamlit as st\nst.title('Spotify Data Explorer')\nst.text('This is a web app to allow exploration of Spotify Charts Data')\n\nimport pandas as pd \nimport numpy as np\n\nfrom pyspark.sql import SparkSession\nimport pyspark.sql.types as t\nimport pyspark.sql.functions as f\nimport seaborn as sns\n\nimport matplotlib.pyplot as plt\n\nadd_selectbox = st.sidebar.selectbox(\n    \"What would you like to look at first\",\n    (\"Big data analysis\", \"Songs\",\"About\")\n)\n\n#df1 = pd.read_csv('/Users/csuftitan/Downloads/charts.csv')\ndf1 = pd.read_csv('https://drive.google.com/file/d/1fUjXJI49cMAsYlnU_l78gKOM5eKqx0os/view?usp=share_link')\ndf1.head()\nst.header('Header of Dataframe')\nst.write(df1.head())\n\nspark = SparkSession.builder.appName(\"spark_app\").getOrCreate()\n\n#df = spark.read.csv(path='/Users/csuftitan/Downloads/charts.csv', inferSchema=True, header=True)\ndf = spark.read.csv(path='https://drive.google.com/file/d/1fUjXJI49cMAsYlnU_l78gKOM5eKqx0os/view?usp=share_link', inferSchema=True, header=True)\n\n\ndf = df.withColumn(\"rank\", f.col(\"rank\").cast(t.LongType())).withColumn(\"date\", f.col(\"date\").cast(t.DateType())).withColumn(\"streams\", f.col(\"streams\").cast(t.IntegerType()))\ndf = df.na.drop()\ndf.count()\ndf.registerTempTable(\"charts\")\n",
      "metadata": {},
      "execution_count": null,
      "outputs": []
    }
  ]
}