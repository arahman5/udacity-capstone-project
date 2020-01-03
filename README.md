# udacity-capstone-project
This repo contains the code for an open ended Udacity capstone project for naturalistic driver behavior studies for development of Autonomous vehicle

# Scope of the project

### Data logging
The scope of the project is to understand driver behaviors by logging various ADAS(Advanced Driver Assistance Systems) signals from sensors such as Stereo cameras, Radars, GPS sensors and internal vehicle signals. The signals are stored in `.vsb` (Vehicle Spy Binary Data Format) and `.db` (Database file) format. These formats are produced by the data logger inside the car. 

### Data Offboarding/ingestion
The data is getting logged from a fleet of cars and each car from the fleet offboards the logged data at the end of the day into a local storage. 

### Data Pipeline Architecture

