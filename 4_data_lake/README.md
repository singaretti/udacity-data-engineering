### Sparkify Data lake

Sparkify is a music streaming startup, and we need to move our data warehouse data to a data lake. All these data are in S3 and contains data about songs of out portifolio and how user interact with this songs and our app.

This project aims to build an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow our analytics team to continue finding insights in what songs their users are listening to.

#### How to use this pipeline?
There are a lot of ways to use this pipeline, it can be in command line of EMR Cluster, or it could be from local machine connected to AWS Account, or another way to do that is using Jupyter Notebook direct from EMR Cluster, you just have to choose to install Jupyter Notebook when creating your cluster.

After create your cluster, attach a new notebook to that cluster and it's and then call functions from `etl.py` in your notebook with your parameters.

#### Star Schema

Output of this process generate five directories in s3 that can be interpreted as "tables", in a star schema with artists, songs, time (of some user activity in app) users, and songplays tables.

#### Songs and Log Data

All data of this project is located in S3 bucket, but you can find a copy here in `data` folder, and to use this directory instead of AWS S3 bucket, it should choose this directory as input data parameter.

#### Examples of data in Star schema

If you want to analyze how data is organized in star schema, you should consider execute commands of `play.ipynb` that read data from s3 (spark.read.parquet).