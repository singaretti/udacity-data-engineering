class SqlQueries:
    
    covid_analysis_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.covid_analysis (
               analysis_id VARCHAR NOT NULL,
               country_name VARCHAR NOT NULL,
               dt date,
               cases INT,
               deaths INT,
               recovered INT,
               processing_date TIMESTAMPTZ,
               CONSTRAINT analysis_pk primary key (analysis_id))
    """)

    covid_analysis_table = ("""
          SELECT * FROM (
          SELECT md5(random()) as analysis_id,
                 'GERMANY' as country_name,
                 to_date(sg.dt,'YYYY-MM-DD') as dt,
                 sum(sg.cases) as cases,
                 sum(sg.deaths) as deaths,
                 sum(sg.recovered) as recovered,
                 getdate() as processing_date
            FROM staging_germany sg
           GROUP BY dt, country_name
        UNION ALL
          SELECT md5(random()) as analysis_id,
                 'ITALY' as country_name,
                 to_date(sir.dt,'YYYY-MM-DD') as dt,
                 sum(sir.totale_casi) as cases,
                 sum(sir.deceduti) as deaths,
                 sum(sir.dimessi_guariti) as recovered,
                 getdate() as processing_date
            FROM staging_italy_regioni sir
           GROUP BY dt, country_name) UNION_TABLE
           ORDER BY dt ASC
    """)

    city_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.city (
               city_id VARCHAR NOT NULL,
               city_name VARCHAR,
               state_id VARCHAR,
               state_name VARCHAR,
               country_id VARCHAR,
               country_name VARCHAR,
               cases INT,
               CONSTRAINT city_pk PRIMARY KEY (city_id))
    """)
    
    city_create_table_tmp = ("""
        CREATE TABLE IF NOT EXISTS public.city_tmp (
               city_id VARCHAR NOT NULL,
               city_name VARCHAR,
               state_id VARCHAR,
               state_name VARCHAR,
               country_id VARCHAR,
               country_name VARCHAR,
               cases INT,
               CONSTRAINT city_pk_tmp PRIMARY KEY (city_id))
    """)
    
    city_table = ("""
        SELECT md5(denominazione_provincia) as city_id,
               denominazione_provincia as city_name,
               md5(denominazione_regione) as state_id,
               denominazione_regione as state_name,
               'ITA' as country_id,
               'ITALY' as country_name
          FROM public.staging_italy_province
         UNION ALL
        SELECT md5(county) as city_id,
               county as city_name,
               md5(state) as state_id,
               state as state_name,
               'GER' as country_id,
               'GERMANY' as country_name
          FROM public.staging_germany
    """)
    
    city_table_append = ("""
        SELECT * FROM (SELECT md5(denominazione_provincia) as city_id,
                              denominazione_provincia as city_name,
                              md5(denominazione_regione) as state_id,
                              denominazione_regione as state_name,
                              'ITA' as country_id,
                              'ITALY' as country_name
                         FROM public.staging_italy_province
                        UNION ALL
                       SELECT md5(county) as city_id,
                              county as city_name,
                              md5(state) as state_id,
                              state as state_name,
                              'GER' as country_id,
                              'GERMANY' as country_name
                         FROM public.staging_germany) union_table
         WHERE city_id NOT IN (SELECT city_id FROM public.city)
    """)

    time_create_table = ("""
        CREATE TABLE IF NOT EXISTS public.time (
               dt DATE NOT NULL,
               "day" INT,
               week INT,
               "month" VARCHAR,
               "year" INT,
               weekday VARCHAR,
               CONSTRAINT time_pk PRIMARY KEY (dt))
    """)
    
    time_create_table_tmp = ("""
        CREATE TABLE IF NOT EXISTS public.time_tmp (
               dt DATE NOT NULL,
               "day" INT,
               week INT,
               "month" VARCHAR,
               "year" INT,
               weekday VARCHAR,
               CONSTRAINT time_pk_tmp PRIMARY KEY (dt))
    """)

    time_table = ("""
        SELECT DISTINCT * FROM (SELECT dt::date as dt,
                                       extract(day from dt::date) as day,
                                       extract(week from dt::date) as week,
                                       extract(month from dt::date) as month,
                                       extract(year from dt::date) as year,
                                       extract(dayofweek from dt::date) as dayofweek
                                  FROM public.staging_italy_regioni
                                 UNION ALL
                                SELECT dt::date as dt,
                                       extract(day from dt::date) as day,
                                       extract(week from dt::date) as week,
                                       extract(month from dt::date) as month,
                                       extract(year from dt::date) as year,
                                       extract(dayofweek from dt::date) as dayofweek
                                  FROM public.staging_germany) UNION_TABLE
    """)
    
    time_table_append = ("""
         SELECT DISTINCT * FROM (SELECT dt::date as dt,
                                        extract(day from dt::date) as day,
                                        extract(week from dt::date) as week,
                                        extract(month from dt::date) as month,
                                        extract(year from dt::date) as year,
                                        extract(dayofweek from dt::date) as dayofweek
                                   FROM public.staging_italy_regioni
                                  UNION ALL
                                 SELECT dt::date as dt,
                                        extract(day from dt::date) as day,
                                        extract(week from dt::date) as week,
                                        extract(month from dt::date) as month,
                                        extract(year from dt::date) as year,
                                        extract(dayofweek from dt::date) as dayofweek
                                   FROM public.staging_germany) UNION_TABLE
          WHERE dt NOT IN (SELECT dt FROM time)
    """)