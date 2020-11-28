**Overview**
    
*This project aims to prepare an environment to analyze how customers enjoy songs using our app, Sparkify.*


**Data Source:**

- Song Data : these files are ingest in songs table and artist table;

- Log Data: these files are necessary to fill time table and users table.

With these four tables, it's possible to use Fact table called Songplays. To keep songplays table updated, it's necessary to keep running etl.py.

**Files:**

```etl.ipynb``` - this file cointains the process step-by-step used to prepare data files to insert into Postgres database. This is not necessary to keep project running, just a brief of what happened and explain about it iteration.

```test.ipynb``` - test file to check if `etl.ipynb` and `etl.py` is working fine. This is also an optional file.

```database_calls.ipynb``` - file that you should use if it's necessary to run all files to populate tables, and basically this file create a database, drop (if exists tables) and create tables. Also, it has a call to etl.py.

```create_tables.py``` - this file contains functions responsible to process database settings, like drop/create database, tables and it's called by `database_calls.ipynb` and it's related to `with sql_queries.py` content.

```sql_queries.py```- here you can find all queries related to "logical" process of create tables, insert tables and select statement. This file is used by `create_tables.py`

```etl.py``` - the last one, but no less important, this file contains all functions responsible to keep project working, it's a version of `etl.ipynb`, but just with functions and calls, no explanation about what should happen in each iteration.