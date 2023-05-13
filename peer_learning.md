# <center>Spark Peer Learning</center>
<hr>

### Jasveen`s approach <br>
#### In data.py <br>
Imported all necessary libraries and created a class **Data** <br>
 
* **load_dataset** Function gets the data from the api and return it.<br>
* **remove_star** function removes the stars that present in the state column in the data.<br>
* **clean_dataset** function is used to clean the data and all the cleaned data are storing in the list **lis**<br>
* **create_dataframe** in this function used spark session to build a dataframe with specified schema and storing dataframe in class variable <br>
<br>
Added **Affected** column in the dataframe <br>
```
def affected(self):
        self.affected_df = self.df.withColumn("Affected",col("Death")/col("Total_cases"))
```
<br>

* To find most affected state performs orderBy on column Affected in descending order, limit the result to one row and selected the State.
```
def most_affected(self):
    self.affected()
    self.affected_df.orderBy(col("Affected").desc()).limit(1).select("State").collect()
```
* To find least affected state performs order by on the column Affected in Ascending order, limiting the result to one row, selecting the State column, and finally collecting the result.
```
def least_affected(self):
    self.affected()
    self.affected_df.orderBy("Affected").limit(1).select("State").collect()    
```
* To find highest cases state, preformed orderby on the Total_cases column in desecnding order , limit the result to one row selecting the State, and finally collecting the result.
```
def highest_cases(self):
    self.df.orderBy(col("Total_cases").desc()).limit(1).select("State").collect()
```

* To find least cases state, preformed orderby on the Total_cases column, limit the result to one row selecting the State, and finally collecting the result.
```
def least_cases(self):
    self.df.orderBy("Total_cases").limit(1).select("State").collect()
```
* To find total cases selected total cases column and performs aggregation function sum() to calculate the total cases
```
def total_cases(self):
        return self.df.select(col("Total_cases")).agg(sum("Total_cases").alias("Total Covid Cases")).collect()
```
* To find most cured state performed orderby on the column Cured in descending order, limit the result to one row selecting the State, and finally collecting the result.
```
def most_cured(self):
        self.cured()
        return self.cured_df.orderBy(col("Cured").desc()).limit(1).select("State").collect()
```
* To find least cured state performed orderby on the column Cured , limit the result to one row selecting the State, and finally collecting the result.

```
 def least_cured(self):
        self.cured()
        return self.cured_df.orderBy("Cured").limit(1).select("State").collect()
```

**In app.py** The routes for all the queries used in data.py were created inside the app.py using Flask Framework and it returned the output in json format.

### Kushagra Singh approach
#### In main.py <br>
Imported all the required libraries and created a spark session, created functions to clean data and to get data from api. <br>
* **get_data()**  function gets the data from the api, removed last two documents from it and returned.<br>
* **create_csv**writes data to a CSV file named output.csv, first writes a header row with column names, and then writes data rows for each state, extracting the values from a dictionary<br>

#### In app.py <br>
Imported all the required libraries and modules, used flask framework to show the results in json format.<br>

**createdataframefromcsv** created a dataframe using spark from csv file.<br>

Created a **TempView** on the dataframe and performed queries on it.

* To get most affected state, selected state and death/confirm from TABLE, order by Affected column in descending order selecting the state and collecting the result
**SELECT state, death/confirm AS affected FROM table").orderBy("affected",ascending=False)** <br>
<br>
* To get least affected state, selected state and death/confirm from TABLE, order by Affected column, selecting the state and collecting the result<br>
**SELECT state, death/confirm AS affected FROM TABLE").orderBy("affected",ascending=True)** <br>
<br>
* To get highest covid cases, performed orderby confirm  in  descending order, selecting state and confirm, limit the results to one row and collecting the result.<br> 
**df.orderBy("confirm",ascending=False).select("state","confirm").limit(1).collect()**<br>
<br>
* To get least covid cases, performed orderby confirm  in ascending order, selecting state and confirm, limit the results to one row and collecting the result.<br> 
**df.orderBy("confirm",ascending=True).select("state","confirm").limit(1).collect()**<br>
<br>
* To calculate total cases used sum function on total column on TABLE. <br>
**SELECT SUM(total) AS TOTALCASES FROM TABLE").collect()** <br>
<br>
* To find most efficient state, performed select state and CURED/CONFIRM  from the TABLE and orderby efficiancy column in desecnding order, limit the result in one row and collecting the result.<br>
**"SELECT STATE , CURED/CONFIRM AS efficiancy from table").orderBy("efficiancy",ascending=False)**<br>
<br>
* To find least efficient state, performed select state and CURED/CONFIRM  from the TABLE and orderby efficiancy column in ascending order, limit the result in one row and collecting the result.<br>
**"SELECT STATE , CURED/CONFIRM AS efficiancy from table").orderBy("efficiancy",ascending=True).**<br>