## Project: Sparkify Data Warehouse
### Introduction
Sparkify has grown their user base and song database and need to move processes and data onto the cloud. Data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

It's necessary to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to continue finding insights in what songs users are listening to.

### Data Warehouse (Redshift) Content
Redshift contains the tables structure below in star schema:


#### Staging tables:
*staging_events*
```
artist VARCHAR,
auth VARCHAR,
firstname VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INT,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId VARCHAR
```
*staging_songs*
```
num_songs INT,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INT
```

#### fact table:
*songplays*
```
songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
start_time BIGINT NOT NULL,
user_id varchar NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
```

#### dimension tables:
*users*
```
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
```

*songs*
```
song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration DECIMAL(10,5)
```

*artists*
```
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT
```

*time*
```
start_time BIGINT PRIMARY KEY,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday VARCHAR
```

### Create Redshift cluster
It's possible to create AWS Redshift cluster using `redshift_creation.ipynb` notebook after fill settings related to the cluster and AWS account in `redshift_cluster.cfg`.

After this cluster is created, it's necessary to have these information related to the cluster in `dwh.cfg` and then, next step is load and insert data into AWS Redshift, it's possible to develop your own way to do that or you can execute `play.ipynb` notebook.

If you got some error while some Redshift statement was running, you cand debug it looking to "stl_load_errors" table, like example below:
```
try: 
    cur.execute("select * from stl_load_errors")
except psycopg2.Error as e:
    print(e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
```