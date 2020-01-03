# udacity-capstone-project
This repo contains the code for an open ended Udacity capstone project for naturalistic driver behavior studies for development of Autonomous vehicles

# Scope of the project

### Data logging
The scope of the project is to understand driver behaviors by logging various ADAS(Advanced Driver Assistance Systems) signals from sensors such as Stereo cameras, Radars, GPS sensors and internal vehicle signals. The signals are stored in `.vsb` (Vehicle Spy Binary Data Format) and `.rosbag` (Robot Operating System) format. These formats are produced by the data logger inside the car. 

### Data Offboarding/ingestion
The data is getting logged from a fleet of cars and each car from the fleet offboards the logged data at the end of the day into a local storage. Each log file consists of 114 signals and length of each log file is 40 minutes which comes resampled at 10Hz, which means there is a datapoint or row produced for each of this signal at every 100ms in the log file. The total number of rows in 1 log file is 2,736,000.
For the purpose of this project 10 log files were used with a combination of the two different raw data formats specified above thus giving a total of 27,360,000 rows. 

### List of signals

### Data model

The data model chosen was a relational database that is a hybrid of star schema and snowflake schema or in others a trade off was made between normalization and denormalization. 

### Data Pipeline Architecture

