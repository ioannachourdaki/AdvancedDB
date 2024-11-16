# Advanced Topics in Database Systems - Semester Project
This Semester Project focuses on the analysis of large datasets using data science techniques. Specifically, we processed Crime Record data for Los Angeles from 2010 to the present, sourced from the publicly available United States government data repository. The Project includes seven distinct tasks, with the corresponding implementations provided in this repository.

## Folders

### (1) code 
It contains the code we implemented for this Project.
- **dataframe.py:** Code for Task 2 > In this step, the Los Angeles Crime Data is loaded, and the schema is defined with the appropriate data types for each column. Using this schema, we create the initial DataFrame. After eliminating any duplicate entries, the DataFrame is saved as a .parquet file to ensure efficient and fast data retrieval.
- **query1.py:** Code for Task 3
- **query2.py:** Code for Task 4
- **query3.py:** Code for Task 5
- **query4.py:** Code for Task 6
- **ex7_q3.py:** Code for Task 7 > Contains modified code for query3.py. The following lines have been added to the file:
```python
crimes_zip_joined=crime_df_trunc.hint("SHUFFLE_REPLICATE_NL").join(revgecoding_df, ['LAT', 'LON'], 'inner')
crimes_zip_joined.explain()
```
These correspond to the **Shuffle Replicate NL** join method. To apply the Broadcast Join, Merge Join, and Shuffle Hash Join methods, we simply need to modify the code as shown below:

**Broadcast Join**
```python
crimes_zip_joined=crime_df_trunc.join(broadcast(revgecoding_df), ['LAT','LON'], 'inner')
crimes_zip_joined.explain()
```
**Merge Join**
```python
crimes_zip_joined=crime_df_trunc.hint("MERGE").join(revgecoding_df, ['LAT','LON'], 'inner')
crimes_zip_joined.explain()
```
**Shuffle Hash Join**
```python
crimes_zip_joined=crime_df_trunc.hint("SHUFFLE_HUSH").join(revgecoding_df,['LAT','LON'], 'inner')
crimes_zip_joined.explain()
```

- **ex7_q4.py:** Code for Task 7 > Contains modified code for query4.py. The following lines have been added to the file:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
```
These correspond to the **Merge Join** method. To apply the **Broadcast Join** method, we simply need to modify the code as shown below:

```python
INNER JOIN (SELECT /*+ BROADCAST(p) */ * FROM Precincts p) p ON c.AREA = p.PREC
```

### (2) data
It contains the datasets used.
- Crime_Data_from_2010_to_2019.csv
- Crime_Data_from_2020_to_Present.csv
- LAPD_Police_Stations.csv
- LA_income_2015.csv
- revgecoding.csv

### (3) files 
It contains the .pdf files of this Project.
- **advanced_db_Project_en.pdf**: Assignment Brief
- **Report.pdf:** Project Report (in Greek)

## Code Execution
To execute the .py files (e.g. filename.py), we first choose the number of Spark executors (e.g. n) and run the code below:
```bash
spark-submit --num-executors <n> --conf spark.log.level=WARN <filename.py>
```
