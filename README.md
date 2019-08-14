# Sparkify Data Lake

### Purpose

Sparkify's growth has led to a significant increase in the size of its user base and song database, prompting a reconsideration of its data architecture. The new architecture moves the data warehouse to a data lake in S3 (AWS simple storage service). Data in this data lake is extracted and transformed with Apache Spark. The transformed data is then converted to parquet files that are stored in S3. The parquet file format uses columnar storage and allows the data to be compressed more efficiently. This transformed data can then be loaded into a data warehouse or processed by a distributed computing / schema on read framework etc. allowing the analytics team to work with a user friendly star schema.

### Schema

The schema for the transformed data stored in parquet files is a star schema. This schema allows the central unit of measurement, songplays, to be stored in a central fact table that also contains references to the context for each songplay event. Seperate dimension tables that haven't been normalised are then created to provide additional information in relation to the references in the fact table. The star schema is easy to understand and its denormalised structure should improve the performance of read queries.

### Files

##### *dl.cfg*

This configuration file contains the AWS access keys required to run the spark application and read from / write to S3.

##### etl.py

This file processes the song and log data files in S3 and transforms the input data to the following star schema.

![Star Schema](/star_schema.png)

**FACT TABLE:**

* songplay - The fact table (songplay) contains songplay data based on user logs and keys to other dimension tables

**DIMENSION TABLES:**

* users - This table contains the first name, last name, gender and level associated with each user_id

* songs - This table contains the song_id, title, artist_id, year, duration for each song

* artist - This table contains the artist_id, name, location, latitude, longitude for each artist

* time - This table breaks down each timestamp (start_time) into hour, day, week, month, year and weekday

These tables are stored as parquet files on S3. The songs table is partitioned by year and artist, the time table file is partitioned by year and month and the songplays table is partitioned by year and month.

To run this file type the following command in to your terminal:

```python

    python etl.py

```
