# Fraud Detection - #Telco #Snowflake #Demo 
Telco Fraud Detection is a Streamlit application designed to show easy-to-understand outcomes. Topic of this dashboards are couple of classic fraud detection scenarios in the Telco industry.

NOTE: this demo pipeline & dashboard is not intended to be a ready-to-use artifact but provides a foundation on available features that can be used to streamline your data trasformation jobs.

![alt text](https://github.com/matteo-consoli/frauddetection/blob/main/screenshot.png?raw=true)

### Dataset Deployment
1) Upload the cdr_dataset.csv.gz to an S3 bucket.
2) Update S3 bucket details in the setup.sql script.
3) Run the setup.sql (HINT: you might also load the dataset to an internal Snowflake stage rather the s3 bucket).
4) During the execution of setup.sql , in order to showcase Snowsight features, in one step is required to upload the country_code.csv via UI in the COUNTRY_CODE table.

### Streamlit in Snowflake Deployment
1) Download "fraud_detection_sis.py" and "logo.png" from the GitHub repository.
2) Create a new Streamlit app on your Snowflake account
3) Paste the code into your new app.
4) Upload the "logo.png" in the Streamlit application stage.
