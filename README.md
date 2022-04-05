# Belong QA Assignment.
-----

Spark based Application to read data from REST endpoint and get insigts and load back to S3/HDFS/Localfile system

1. Read Data from Restful API
2. Convert Input JSON into to Dataframe
3. Process Structured data and get the insights for 

    - Top 10 (most pedestrians) locations by day
    - Top 10 (most pedestrians) locations by month
4. Process predictions and write into S3 (in cloud and to Local filesystem while running tests)
5. Write input data into local

* All the listed artifacts above will be produced as part of application build.

Run the project using commands listed in this README and proceed to:
1. Write Unit tests for each module - To be done.
2. Write Integration tests for each functionality - To be done.
3. Write test scenario documentation and produce html docs for code coverage - To be done.


Once above completed.

4. Write results to database table (Hint: postgres docker on local)
5. Use soda sql to define tests (https://docs.soda.io/soda-sql/tests.html)

Application Properties
----------------------
Application properties to process data from REST API and process

```properties

#Source Options
sensorCountURL=https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json
sensorLocationURL=https://data.melbourne.vic.gov.au/resource/h57g-5234.json

#Target Options
# you might have to change the s3 bucket to point to your own
targetURI=s3a://predictions-qa-landing/data/ | file:///c/tmp/ | hdfs://data/predictions-qa-landing  -- S3/Localfile/HDFS
writerFormat=parquet | ORC | avro | CSV | json
targetMode=append | overwrite
```


## Build Instructions

Java sdk version 8 to 11 preferred
Download spark 3.1.1: https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
```bash
./gradlew clean build shadowJar

 
```

Artifact Location
----------------

```<Project Root>/build/libs```

Results:
-------
Table 1 -  daily top 10 predictions

Table 2 - monthly top 10 Predictions



Production Execution Instructions
----------------------------------

```
 <<path to spark home 3.1.1>>/bin/spark-submit  --class io.github.qa.belong.assignment.Launcher build/libs/assignment-all.jar build/resources/main/predictions.properties

  more on spark submit can be found at https://spark.apache.org/docs/latest/submitting-applications.html
```


## License
[Apache](http://www.apache.org/licenses/LICENSE-2.0.txt)