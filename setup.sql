-- # Telco Fraud Detection - Data Preparation for Streamlit App.
-- # Author: Matteo Consoli 
-- # v.1.0 - 10-10-2023

-- We'll run this demo as accountadmin. 
-- Do you want to discover more about Data Governance? 
-- Check this out: https://docs.snowflake.com/en/user-guide/security-access-control-overview

-- Create Database and Schema Objects
USER ROLE ACCOUNTADMIN;
CREATE DATABASE FRAUD_DATA;
CREATE SCHEMA FRAUD_DATA.ANALYSIS;
USE SCHEMA FRAUD_DATA.ANALYSIS;

-- Create Warehouse to run COPY commands and execute queries
CREATE OR REPLACE WAREHOUSE FRAUD_ANALYSIS 
WAREHOUSE_SIZE = 'XSmall' 
AUTO_SUSPEND=60 
AUTO_RESUME=True; 

-- -- -- -- -- -- -- -- --
-- Data Engineering     --
-- -- -- -- -- -- -- -- --

-- Default File Format for CSV
CREATE OR REPLACE FILE FORMAT cdr_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true
  SKIP_BLANK_LINES = TRUE  ;

-- Storage integration with S3 bucket where we placed our files (adjust parameters below accordingly)
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/s3_demo_webinar_data_access'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://sf-se-corporate-team/demo_telco_webinar/');

-- Create External Staging in Snowflake linked to the reference s3 bucket.
CREATE OR REPLACE STAGE CDRS_STAGING
  STORAGE_INTEGRATION = s3_int
  URL = 's3://sf-se-corporate-team/demo_telco_webinar/'
  FILE_FORMAT = cdr_csv_format;

--Check Storage Integration and list files in the s3 bucket.
list @CDRS_STAGING;

-- Create Tables
CREATE TABLE ANALYSIS.CDRS_STAGING (
DURATION NUMBER,
TYPE NUMBER,
ANUM VARCHAR(100),
BNUM VARCHAR(100),
YEAR VARCHAR(100),
MONTH VARCHAR(100),
DAY VARCHAR(100),
TIMEKEY VARCHAR(100),
IMEI VARCHAR(100),
RATE VARCHAR(100),
CCODE VARCHAR(100),
DIRECTION INTEGER
);

CREATE OR REPLACE TABLE SIM_ACTIVATION (
  MSISDN VARCHAR(100),
  ACTIVATION_DATE DATETIME,
  IMEI VARCHAR(100)
  );

CREATE TABLE HOTLIST_NUMBER (
  MSISDN VARCHAR(100),
  COUNTRY VARCHAR(100)
);

-- Load CDRs from External Stage to CDRS_STAGING TABLE
COPY INTO CDRS_STAGING
  FROM @CDRS_STAGING;

-- You can use FrostyGen to create additional records for the SIM ACTIVATION table or use the query below to generate dummy records from CDRS loaded.
INSERT INTO SIM_ACTIVATION 
SELECT * FROM (
  SELECT DISTINCT(ANUM) AS A_NUM, dateadd(day, -uniform(20, 500, random()), current_date()) AS ACT_DAY,IMEI
  FROM CDRS_STAGING)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A_NUM ORDER BY ACT_DAY) = 1;

-- -- -- -- -- -- -- -- --
-- UI - Data Loading    --
-- -- -- -- -- -- -- -- --

-- This step must be done using the UI - DO NOT SKIP IT (showcasing data loading feature from UI)
-- Country Code (Static Table): From Snowsight you can upload CSV directly into a table. 
-- We'll use this feature to load data in the HOTLIST_NUMBER table. 
-- 1) Use the file country_code.csv and load it via Snowsight UI.
-- 2) Create a new table from the UI based on the csv mentioned above. Table should be named "COUNTRY_CODE" otherwise next command will fail.
-- More details: https://docs.snowflake.com/en/user-guide/data-load-web-ui
  
-- Data Engineering: Create Dynamic Tables and automatically run the first refresh.
CREATE OR REPLACE DYNAMIC TABLE CDRS_NORMALISED
    TARGET_LAG = '1 day'
    WAREHOUSE = FRAUD_ANALYSIS
    AS
    SELECT  
        TO_TIMESTAMP( 
            CONCAT(YEAR, 
            CASE WHEN LENGTH(MONTH)=1 THEN CONCAT(0,MONTH) ELSE MONTH END, 
            CASE WHEN LENGTH(DAY)=1 THEN CONCAT(0,DAY) ELSE DAY END, 
            TIMEKEY
            ), 'YYYYMMDDHH24MISS'
        ) as CDR_TIMESTAMP, 
        ANUM, 
        BNUM, 
        RATE,
        DURATION,
        IMEI,
        CCODE as B_COUNTRY_CODE,
        CASE WHEN TYPE=1 THEN 'SMS' ELSE 'Voice' END as CDR_TYPE,
        CASE WHEN DIRECTION=1 THEN 'Incoming' ELSE 'Outgoing' END CDR_DIRECTION
        FROM CDRS_STAGING;

ALTER DYNAMIC TABLE CDRS_NORMALISED REFRESH;

CREATE OR REPLACE DYNAMIC TABLE CDRS_ENRICHED
    TARGET_LAG = '1 day'
    WAREHOUSE = FRAUD_ANALYSIS
    AS
        SELECT cn.*, COUNTRY_NAME, RISK_SCORE as COUNTRY_RISK_SCORE, ACTIVATION_DATE as A_NUM_ACTIVATION_DATE 
        FROM CDRS_NORMALISED cn 
        left join SIM_ACTIVATION sa on cn.anum = sa.MSISDN 
        left join COUNTRY_CODE cc on cn.b_country_code = cc.country_code ;
        FRAUD_DATA.ANALYSIS.TESTFRAUD_DATA.ANALYSIS.TEST
    ALTER DYNAMIC TABLE CDRS_ENRICHED REFRESH;

-- -- -- -- -- -- -- -- --
-- Data Analytics Demo  --
-- -- -- -- -- -- -- -- --

-- Use Case: Based on the country codes with highest risk score, create a list of top-risk hotlist numbers.  
INSERT INTO HOTLIST_NUMBER
  SELECT DISTINCT BNUM, COUNTRY_CODE FROM CDRS_STAGING  cs join COUNTRY_CODE cc ON cs.CCODE = cc.COUNTRY_CODE
  WHERE RISK_SCORE > 4
  ORDER BY RISK_SCORE DESC LIMIT 100;

-- It's all set now to deploy the Streamlit App in Snowflake and run some fraud  analytics.
