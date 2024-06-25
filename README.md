# Rearc Data Quest

### Part 1: AWS S3 & Sourcing Datasets

1. The dataset is hosted [here](https://edgeofjupiter.s3.amazonaws.com/rearc-data/bls/index.html)
2. I created a Lambda function [sync_bls_data](snyc_bls_data/lambda_function.py) to automate the data sync. The script compares files that are downloaded from the source with a SHA256 checksum. Newly added or updated files are added to the S3 bucket. Missing file names are presumed to be deleted and are removed from the S3 bucket.

### Part 2: APIs

This [script](fetch_population_data/lambda_function.py) fetches the data from the API and stores as a JSON file in an S3 bucket.

### Part 3: Data Analytics

The notebook with the data analysis is [here](data_analytics.ipynb). Note: for this notebook I sourced the data files locally instead of remotely in my S3 bucket.

### Part 4: Infrastructure as Code & Data Pipeline with AWS CDK

I quickly found that I was unable to create a Lambda layer with a dependency for Pandas, which was my choice for dataframes in part 3. I attempted to add the Pandas layer provided by AWS to my function and the consumption size for the layer was too large. In the interests of expediency, I decided not to pursue this step further.

If I were to create a data pipeline in AWS for this step, I would:

- Add a step function to execute the data file downloads from steps 1 and 2 as a scheduled job. The downloaded files would be written to S3.
- On successful completion of the downloads, write a message to a SQS queue.
- The consumer of the queue would spin up an ECR instance, from an image that has the required dependencies for data analysis.
- Write the results of the data analysis to a database for further processing.
