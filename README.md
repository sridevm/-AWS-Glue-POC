# -AWS-Glue-POC
Using AWS Glue Studio to create jobs from a data source, perform a transformation of that data, and save the result set in a data target.
##  To analyze arrival data for major air carriers to calculate the popularity of departure airports month over month. 
Step 1 : Added a Crawler -- Crawler converts the flights data for the year 2016 in CSV format stored in Amazon S3 to Table format.
Step 2 : To create a Job  -- Selected S3 for the Source and S3 for the Target.
Step 3 : Edit the data source node -- Naming the Source node as S3 Flight Data and selected flights-db for Database and flightscsv for table from AWS Glue Data Catalog.
Step 4 : Edit the transform node  -- dropped the two columns quarter and day_of_week and Changed the data type for the month and day keys to tinyint.
Step 5 : Edit the data target node --Naming the Target node as Revised Flight Data and the format is JSON .The target location is S3.
Job has been created and Scheduled to run for every hour.
