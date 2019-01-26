##### I written code within two days for Screening Test conduct by TrustingSocial, and also i included some functional testing.

Problem statement
=================
####I. Data preprocessing:
1. Mask tower sensitive information. [Anonymize data]
2. Look for loss of data during ingestion.
3. Deduplication of records during ingestion.
####II. Data transformation:

1. Find peak time of total activity for the telecom on a daily basis.
 - Assume peak time to be of an interval of 1 hour.
2. Find anomalies in the average data transferred per square ID.
 - If the activity for a square ID increases above the Mean value, log the activity as anomalous.
 - Assume a period of 7 days of activity
3. Find the hourly peak usage per square ID.
4. Rank the square IDs/per day based on their overall activity.
5. Find the average activity for a cell square per day.
6. Categorize countries based on their data consumption, call activity and SMS activity
(high/medium/low).
 - Create three consecutive ranges, label them high, medium and low in
decreasing order. Set the range limits by yourself.
 - For example:- If the three ranges are 15-11, 10-6, 5-1, and the overall activity of
a country is 12, then mark it as high.


###Sample dataset
  - Follow the instructions here(https://dandelion.eu/datagems/SpazioDati/telecom-sms-call-internet-tn/resource/) to download the public dataset available


CDRs ( Call detail records) Analysis
====================================


### Requirements
- scala v2.11.*
- java v1.8
- sbt

- spark v2.3.2
- Hadoop v2.* or 3.*


### USAGE ###

- Build Jar by using ``` sbt clean compile package```

- generates jar file ```cdr_etl_2.11-0.1.0-SNAPSHOT.jar``` in the directory ```/target/scala-2.11/```

- Main method : ```com.organization.ts.CDRAnalysisMainCDRAnalysisMain```. And Run Spark Application in Yarn Cluster mode.


    ``` spark-submit --master yarn --jars ./scopt_2.11-3.7.1.jar --class "com.organization.ts.CDRAnalysisMain" ./target/scala-2.11/cdr_etl_2.11-0.1.0-SNAPSHOT.jar --process="task6" --path="/preprocessed_data/"```

- Running Spark Application in Standalone mode :
``` spark-submit --jars ./scopt_2.11-3.7.1.jar --class "com.organization.ts.CDRAnalysisMain" ./target/scala-2.11/cdr_etl_2.11-0.1.0-SNAPSHOT.jar --process="task6" --path="/preprocessed_data/"```

1. Additional jars ``` scopt_2.11-3.7.1.jar ```

2. Required two arguments ``` --process ```, and ``` --path```
    
    - ```Process``` : It could be 
        
        - ```data-copy``` (Recommend - Do Not use this or If want this please look carefully input path and output path, it does recursively): this argument is used to copy data from local ``` CDR``` files directory to HDFS, and please input path directory at ```Constants.LOCAL_PATH ``` and give outpuut path directory to save data in HDFS ``` --path ```
        
        - ```prep-processing``` (Recommend - use this) : Its used to clean, structure and compress the data with parquet. Let say all ``` CDR ``` available in HDFS in path ```/input_data/``` pass this as a second arg at ```--path```, and this process saves the result data at ```/preprocessed_data/ ```.
        
        - ```all```  (Recommend - use this) : After pre-processing data (cleaning), use this process. And its generates results for all Tasks, along this need use ``` --path=/preprocessed_data/``` where cleaned data present.
        
        - ```task1```  (Recommend - use this) : After pre-processing data (cleaning), use this process. It creates test task one results and save in ``` /task1/ ```, along need use ``` --path=/preprocessed_data/``` along process arg.
        
        - ```task2```  (Recommend - use this) : After pre-processing data (cleaning), use this process. It creates test task two results and save in ``` /task2/ ```, along need use ``` --path=/preprocessed_data/``` along process arg.
        
        - ```task3```  (Recommend - use this) : After pre-processing data (cleaning), use this process. It creates test task three results and save in ``` /task3/ ```, along need use ``` --path=/preprocessed_data/``` along process arg.
        
        - ```task4```  (Recommend - use this) : After pre-processing data (cleaning), use this process. It creates test task four results and save in ``` /task4/ ```, along need use ``` --path=/preprocessed_data/``` along process arg.
        
        - ```task5```  (Recommend - use this) : After pre-processing data (cleaning), use this process. It creates test task five results and save in ``` /task5/ ```, along need use ``` --path=/preprocessed_data/``` along process arg.
        
        - ```task6```  (Recommend - use this) : After pre-processing data (cleaning), use this process. It creates test task six results and save in ``` /task6/ ```, along need use ``` --path=/preprocessed_data/``` along process arg.
        
    - ```path``` (Recommend - use this) : Its HDFS path, to process ```data-copy```  could be ```/input-data/```, and to pre-processing, it could be ```/input-data/```, and remaining all process it could be ``` --path=/preprocessed_data/```.
    
    
    
### Final USAGE 
- Let say you have ```CDR``` data in HDFS Path ```/input-data/```, and build the jar file and Clean, Structure and Compress the data by using Pre-processing functionality, ```--process="pre-processing""``` with ```--path=""/input-data/"```.

    - Standalone : ``` spark-submit --jars ./scopt_2.11-3.7.1.jar --class "com.organization.ts.CDRAnalysisMain" ./target/scala-2.11/cdr_etl_2.11-0.1.0-SNAPSHOT.jar --process="pre-processing" --path="/input-data/"``` 
    
    - Cluster : ``` spark-submit --master yarn --jars ./scopt_2.11-3.7.1.jar --class "com.organization.ts.CDRAnalysisMain" ./target/scala-2.11/cdr_etl_2.11-0.1.0-SNAPSHOT.jar --process="pre-processing" --path="/input-data/"``` with proper executor, driver and overhead memory.
    
- After above process completed, then run spark application to get tasks results.  

    - Standalone : ``` spark-submit --jars ./scopt_2.11-3.7.1.jar --class "com.organization.ts.CDRAnalysisMain" ./target/scala-2.11/cdr_etl_2.11-0.1.0-SNAPSHOT.jar --process="pre-processing" --path="/preprocessed_data/"``` 
    
    - Cluster : ``` spark-submit --master yarn --jars ./scopt_2.11-3.7.1.jar --class "com.organization.ts.CDRAnalysisMain" ./target/scala-2.11/cdr_etl_2.11-0.1.0-SNAPSHOT.jar --process="all" --path="/preprocessed_data/"``` with proper executor, driver and overhead memory.
    
		- Add executor-memory, no of executors, executor-memoryOverhead, executor-cores, driver-memory and driver-memoryOverhead.
    
    - If you want run Spark Applications with task by task, than use ```--process="task1""```, ```--process="task2""```  so on.
    
    
### Testing
- Done some functional testing
- To testing
    ```sbt test```, OR to see particular test results,can use this
    ```sbt "test:testOnly *CDRAnalysisTest"```    
