## Data Pipelines with Apache Airflow

### Intro
---
Course: Udacity Nanodegree Data Engineering <br>
Project: Project 5 - Data Pipelines with Airflow <br>
Owner: Mihaly Garamvolgyi <br>
Date: 2023-01-02 <br>

A fictional data warehouse for a startup named Sparkify to analize their song and user data in their database. Pipeline task.

### Description
---
An Apache Airflow based application that reads logfiles and song data from Amazon S3 - transforms data and stores the created tables in Redshift database in AWS. 

The data loads are scheduled and orchestrated in Airflow application. 

### Project Structure
---

```
.
├── dags                # folder containing the DAG file for Airflow
├── plugins             # Operators and helpers for running the DAG
├── create_tables.sql   # Main
└── README.md
```

### Dependencies
---
- Python 3
- apache-airflow


### Schema
---
#### Fact table: <br>
`songplays` - records in log data associated with song plays i.e. records with page NextSong <br>
`songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`


#### Dimension tables: <br>
`users` - users in the app <br>
user_id, first_name, last_name, gender, level
songs - songs in music database<br>
`song_id`, `title`, `artist_id`, `year`, `duration`
artists - artists in music database<br>
`artist_id`, `name`, `location`, `lattitude`, `longitude`
time - timestamps of records in songplays broken down into specific units<br>
`start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`




### How to run
---
1. Connections `aws_credentials` and `redshift` should be created in Airflow
2. Start and End dates should be specified in `sparkify_dag.py`
2. Run `sparkify_dag.py` using Aiflow