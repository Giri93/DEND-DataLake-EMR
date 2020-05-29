# PROJECT DESCRIPTION:This project is about creating a data lake sparkifydl for sparkify music streaming app. 

Below are the files in the project:
**dwh_cfg:** Contains the configurations parameters to connect to S3, EMR cluster
**etl.py:** To connect to input _S3 bucket_, process all the json files and write the files to output _S3 bucket_
**Test.ipynb:** To trigger the .py file and test the bits of code required.

The source data (_s3://udacity-dend/song_data_, _s3://udacity-dend/log_data_) from S3 is in JSON format. We have dimension tables (_songs_, _artists_, _users_, _time_) which are loaded with the data which is processed from the source JSON files. We have created a Fact table (_SongPlays_) loaded with the data from dimension files and source data. All the dimension tables and Fact tables are written in parquet format with partitions to the path provided,