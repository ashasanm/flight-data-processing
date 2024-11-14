Airport and Flight Data Processing with Apache Spark, SQL, and Docker
Objective
The objective of this project is to develop a scalable solution to process large datasets generated by various sources, such as FlightRadar. The system will process airport and flight data, store results in a SQL database, and package the entire solution with Docker for easy deployment and scalability.

Project Overview
The project is divided into the following parts:

Data Processing with Apache Spark
Storing Processed Data in a SQL Database
Dockerizing the Application
Key Requirements
Data Processing: Process large datasets (e.g., adsb.json and oag.json) using Apache Spark. Clean, aggregate, and transform the data to perform analysis like computing delayed flights.
SQL Database: Design a relational database model to store the processed data and ensure efficient querying for future analysis.
Dockerization: Containerize the entire application, including Apache Spark, the SQL database (PostgreSQL), and any other necessary dependencies, into Docker containers for easy deployment.
Dataset
The dataset consists of two primary JSON files:

adsb_multi_aircraft.json: Contains flight information for multiple aircraft.
oag_multiple.json: Contains operational flight data from OAG (Official Aviation Guide).
These datasets will be processed using Apache Spark, and the results will be stored in a PostgreSQL database.

Installation & Setup
Prerequisites
Before setting up the project, ensure you have the following installed:

Docker and Docker Compose
Python 3.x
Apache Spark (will be managed via Docker)
PostgreSQL (also managed via Docker)
Celery (for asynchronous task processing)
Step-by-Step Setup

Step-by-Step Setup
1. Clone the Repository
bash
Salin kode
git clone https://github.com/yourusername/flight-data-processing.git
cd flight-data-processing

2.Setup ENV


3. Docker Setup
Build and start the application with Docker Compose:

bash
Salin kode
docker-compose up --build