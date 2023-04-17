## Pyspark Assignment

Imported all necessary modules

```
import requests
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from key import get_env_variable
import os
import glob
import pandas as pd
from datetime import datetime
import shutil
```

<p> Fectch data from api using requests module</p>

```
def get_data():
    covid_details_api_url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": KEY,
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", covid_details_api_url, headers=headers)
    return response
```

<p>Created RDD using sparkcontext</p>


```
def get_rdd():
    rdd = sc.parallelize(api_response)
    return rdd
```

<p> Created dataframe using spark sql and cleaned it</p>
```
def create_and_clean_df(rdd):
    df = spark.read.json(rdd)
    df = df.withColumn("confirm", col("confirm").cast(IntegerType())).\
        withColumn("cured", col("cured").cast(IntegerType())).\
        withColumn("state", regexp_replace("state", "\*", "")).\
        drop("_corrupt_record").\
        filter(df["state"] != "")
    df = df.select('slno', 'state', 'confirm', 'cured', 'death', 'total')
    return df
```

<p> Created a temp view to excecute sql quaries</p>

```
data=create_and_clean_df(get_rdd())
df1 = data.createOrReplaceTempView("tableA")
```


<p> Stored data into file </p>

```
def csv_file():
    dt = datetime.now()
    data.write.format("csv").option("header", "true").save(fr"/Users/mohangundluri/Desktop/pyspark_data_{dt}.csv")
    # set the directory where the csv files are saved
    csv_dir = fr"/Users/mohangundluri/Desktop/pyspark_data_{dt}.csv"

    # read all the csv files into a list
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))

    # create an empty list to store the DataFrames
    dfs = []

    # append each DataFrame to the list
    for csv in csv_files:
        df = pd.read_csv(csv)
        dfs.append(df)

    # concatenate all the DataFrames into a single DataFrame
    combined_df = pd.concat(dfs)

    # write the combined DataFrame to a new csv file
    combined_df.to_csv(fr"/Users/mohangundluri/Desktop/files/combined_results_{dt}.csv", index=False)
    shutil.rmtree(fr"/Users/mohangundluri/Desktop/pyspark_data_{dt}.csv")
    return fr"/Users/mohangundluri/Desktop/files/combined_results_{dt}.csv"

```

<p>Excecuted all the sql queries and stored all the results in a list</p>

```
ans = list([most_affected_state,
       least_affected_state,
       highest_covid_cases,
       least_covid_cases,
       total_cases,
       most_efficient_state,
       least_efficient_state
])
```

<p> Used flask framework to show the results</p>