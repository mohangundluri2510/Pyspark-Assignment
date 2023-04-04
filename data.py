import requests
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from key import get_env_variable


KEY = get_env_variable()


def get_data():
    covid_details_api_url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": KEY,
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", covid_details_api_url, headers=headers)
    return response


api_response = get_data().json().values()


spark = SparkSession.builder.master('local[*]').getOrCreate()
sc = SparkContext.getOrCreate()


# def clean(line):
#     try:
#         if line['state']:
#             if line['state'] != "":
#                 return line
#     except:
#         pass


def get_rdd():
    rdd = sc.parallelize(api_response)
    return rdd



def create_and_clean_df(rdd):
    df = spark.read.json(rdd)
    df = df.withColumn("confirm", col("confirm").cast(IntegerType())).\
        withColumn("cured", col("cured").cast(IntegerType())).\
        withColumn("state", regexp_replace("state", "\*", "")).\
        drop("_corrupt_record").\
        filter(df["state"] != "")
    return df

data=create_and_clean_df(get_rdd())
df1 = data.createOrReplaceTempView("tableA")



sql_query1 = """
                 SELECT state, (death/total)  Death_Rate
                 FROM tableA
                 Order by Death_Rate DESC
                 Limit 1;
            """
most_affected_state = spark.sql(sql_query1).collect()[0][0]


sql_query2 = """
                 SELECT state, (death/total)  Death_Rate
                 FROM tableA
                 Order by Death_Rate
                 LIMIT 1;
            """
least_affected_state = spark.sql(sql_query2).collect()[0][0]


sql_query3 = """
            SELECT state, total
            FROM tableA
            ORDER BY total DESC
            LIMIT 1;
        """
highest_covid_cases = spark.sql(sql_query3).collect()[0][0]



sql_query4 = """
            SELECT state, total
            FROM tableA
            ORDER BY total
            LIMIT 1;
        """
least_covid_cases = spark.sql(sql_query4).collect()[0][0]


sql_query5 = """
                 SELECT sum(total) Total_Cases
                 FROM tableA
            """
total_cases = spark.sql(sql_query5).collect()[0][0]



sql_query6 = """
                 SELECT state, cured/total as Efficient_rate
                 FROM tableA
                 ORDER BY Efficient_rate DESC 
                 LIMIT 1;
            """
most_efficient_state = spark.sql(sql_query6).collect()[0][0]



sql_query7 = """
                     SELECT state, cured/total as Efficient_rate
                     FROM tableA
                     ORDER BY Efficient_rate
                     LIMIT 1;
                """
least_efficient_state = spark.sql(sql_query7).collect()[0][0]


ans = list([most_affected_state,
       least_affected_state,
       highest_covid_cases,
       least_covid_cases,
       total_cases,
       most_efficient_state,
       least_efficient_state
])