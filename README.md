## **Data WH on Redshift**

### AWS Infrastructure

Infrustructer for the future Data Warehouse is created automatically via AWS python wrapper boto3

### Input Data

The data comes from two sources

    1. Million Songs Database
    2. Radio Log Data (below is an example of log data)
  
   ![Image of Log](https://github.com/Ekovarskiy/DataWharehouse_Redshift/blob/master/log-data.png)

Data resides on Amazon S3 instance and the data for analysis will be stored on Redshift. To automate ETL pipline it was implemented via Apache Airflow. The scheme is shown below

![Image of Dag](https://github.com/Ekovarskiy/DataWharehouse_Redshift/blob/master/example-dag.png)

### The purpose of the database

Analytics are intersted in the analysis of the songs users of their service listen to, the purpose of this analysis is probably to understand
    - What songs and artists are the most popular
    - When do users listen to music, is there any seasoning
    - What is the geographical distribution of music preferencies
    - etc.
Therefore the database is needed in order to facilitate and speed up such analysis.


### Database Structure

The event (fact) in the log can be described as Some **User** listens to some **Song** by some **Artist** at **Time**,  therefore it is logical to use Star Schema, where the fact table (**SongPlay_Table**) would contain information on the events and a series of dimension tables would contain details of theses events, such as **Song_Table** description, **Artist_Table** information, **User_Table** information and **Time_Table**. Also such structure reflects aspects Sparkify Analytics are interested in, so their potential queries would be easier to perform.

It is worth mentioning that the same manner **Timestamp** column from **SongPlay_Table** is parsed into **Year, Month, Day, etc.** the column **Location** can be parsed into **State, City, Street** columns to make geospatial analysis easier.

To optimize query time the following distribution strategy was implemented

 - Since the main objective of the future analysis is to understand what songs Sparkify Users listen to, it is safe to assume that **Songplay_table** and **Song_table** will be used for joins a lot and additionally, among all table these 2 are the biggest, so it is inefficient to copy them on all nodes. Because of that **Songplay_Table** and **Song_table** were distributed by **Song_id**. Additionally, the former was sorted by **start_time** and the latter by **artist_id**
 - For other tables due to their relatively small size **"DistStyle ALL"** was chosen. The sortkeys for **Artist** and **Time** tables are **artist_id** and **start_time** respectively.
 - It is worth mentioning that at some point as the number of users of the service encreases, it will be necessary to change distribution strategy of user table


### ETL Structure

ETL consists of the following steps:
1. Redshift cluster and corresponding AIM Role are launched using python API boto3 (RedshiftClusterInit.ipynb);
2. 7 Tables are created, that is 2 staging tables and 5 tables for star schema with distribution strategies described above;
3. 2 Staging tables are copied from the corresponding resources on S3;
4. Using 2 staging tables the rest of the tables are injested using corresponding insert statements with conditions to avoid duplicates;
5. Once star schema tables are populates staging tables are droped.


### Possible Queries

1. Compare preferencies of paid and free users

        QUERY = """SELECT level, s.title FROM songplay_table sp
                JOIN song_table s ON sp.song_id=s.song_id
                GROUP BY level;"""

2. Check user preferencies in particular year and month depending on weekday

        QUERY = """SELECT s.title FROM (song_table as s
                JOIN (songplay_table as sp JOIN time_table as t ON sp.start_time=t.start_time)
                ON sp.song_id=s.song_id)
                WHERE year=2018 AND month=11
                GROUP BY weekday;"""
    
