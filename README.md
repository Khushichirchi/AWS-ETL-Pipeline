# AWS-ETL-Pipeline
This ETL pipeline uses AWS resources for data processing and monitoring. It performs transformations like dropping columns etc. and saves the output in Parquet format to an S3 destination bucket.
ETL Pipeline Workflow
1️⃣ Extract – Load three raw CSV datasets from the staging S3 bucket.
2️⃣ Transform – Use AWS Glue to:

Join datasets using a unique ID.
Drop unnecessary columns.
Convert data to Parquet format.
3️⃣ Load – Save the transformed data into the data_warehouse S3 bucket.
4️⃣ Query – Use AWS Athena (Query Editor) to retrieve the necessary data.




