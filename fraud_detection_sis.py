import streamlit as st
import snowflake.connector as sf
import pandas as pd
import altair as alt
import base64
from snowflake.snowpark.context import get_active_session

# Fraud Detection Analytics - SiS 
# Author: Matteo Consoli 
# v.1.0 - 10-10-2023

### Functions  ###
def fetch_data(query):
    session = get_active_session()
    df = session.sql(query).to_pandas()
    return df

### Improvement Tips ###
### NOTE: It's possible to add data pickers and other data filtering options
### to dynamically updates dashboards displayed and re-run parametrised queries below
### ---------------- ###

### Query Catalog to extract analytics info to display on Dashboards
query_riskcountry = '''
        SELECT COUNTRY_NAME as CountryName, 
        CASE WHEN RISK_SCORE<5 THEN 'Medium' ELSE 'High' END as RiskScore 
        FROM FRAUD_DATA.ANALYSIS.COUNTRY_CODE 
        WHERE RISK_SCORE>=4
    '''

query_top5country = '''
        SELECT COUNTRY_NAME as CountryName, 
        COUNT(*) as TotalCount, 
        SUM(RATE) as TOTALRATE , 
        SUM(DURATION)/1000 as TotalDuration 
        FROM FRAUD_DATA.ANALYSIS.CDRS_ENRICHED 
        WHERE COUNTRY_RISK_SCORE>=4 and RATE>0 AND CDR_DIRECTION ='Outgoing'
        AND CDR_TYPE = 'Voice'
        GROUP BY COUNTRY_NAME;
    '''

query_TopCalledNumbers = '''
        SELECT BNUM as BNUMBER, B_COUNTRY_CODE as COUNTRYCODE, COUNTRY_NAME as CountryName, count(*) as TotalCount, SUM(RATE) AS TOTALRATE, SUM(DURATION) as TOTALDURATION FROM 
        FRAUD_DATA.ANALYSIS.CDRS_ENRICHED 
        WHERE COUNTRY_RISK_SCORE>=4 AND CDR_DIRECTION ='Outgoing'
        AND CDR_TYPE = 'Voice'
        GROUP BY BNUM, COUNTRYCODE, COUNTRY_NAME
        ORDER BY COUNT(*) DESC 
        LIMIT 100
    '''

query_SMS_Spamming_by_Country = '''
        SELECT COUNTRY_NAME as CountryName, count(*) as TotalCount FROM 
        FRAUD_DATA.ANALYSIS.CDRS_ENRICHED 
        WHERE CDR_TYPE = 'SMS' AND COUNTRY_RISK_SCORE<=3
        GROUP BY COUNTRY_NAME 
        ORDER BY COUNT(*) DESC;
    '''

query_SMS_Spamming = '''
        SELECT BNUM as BNumber, B_COUNTRY_CODE as COUNTRYCODE, COUNTRY_NAME as CountryName, count(*) as TotalCount FROM 
        FRAUD_DATA.ANALYSIS.CDRS_ENRICHED 
        WHERE CDR_TYPE = 'SMS' AND COUNTRY_RISK_SCORE<=3
        GROUP BY BNUM,B_COUNTRY_CODE,COUNTRY_NAME
        HAVING COUNT(*)>=100
        ORDER BY COUNT(*) DESC;
    '''

### Query Execution ###
query_data_1 = fetch_data(query_top5country)
query_data_2 = fetch_data(query_riskcountry)
query_data_3 = fetch_data(query_TopCalledNumbers)
query_data_4 = fetch_data(query_SMS_Spamming_by_Country)
query_data_5 = fetch_data(query_SMS_Spamming)


### Dashboard UI ###
st.title("Fraud Detection Analytics")
st.write('Demo Dashboards powered by Snowflake using Streamlit')
st.markdown("""----""")

# Sidebar filter affecting main view
image_name = 'logo.png'
mime_type = image_name.split('.')[-1:][0].lower()        
with open(image_name, "rb") as f:
    content_bytes = f.read()
content_b64encoded = base64.b64encode(content_bytes).decode()
image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
st.sidebar.image(image_string)

if 'selection' not in st.session_state:
    st.session_state['selection'] = 0

selected = st.sidebar.selectbox("Select a Dashboard", ("Risky Destination Analysis", "SMS Spamming", "Handset Fraud (Empty)", "Wangiri (Empty)", "SIM Swap (Empty)"), index=st.session_state['selection'])

# Conditional display of data visualization
if selected == 'Risky Destination Analysis':
    st.subheader("Risky Destination Analysis")
    st.write("Top 5 Risky Countries with highest number of outgoing calls")
    st.bar_chart(data=query_data_1, x="COUNTRYNAME", y=["TOTALCOUNT", "TOTALRATE", "TOTALDURATION"])
    st.write('Top International Called Numbers (Risky Countries)') 
    st.dataframe(query_data_3.set_index(query_data_3.columns[0]),width=1200)

if selected == 'SMS Spamming':    
    st.subheader('SMS Spamming')
    st.write('Top Countries with highest number of SMS sent to subscribers')    
    st.area_chart(data=query_data_4, x="COUNTRYNAME", y="TOTALCOUNT")
    st.write('Top International Numbers sending SMS to Subscribers')
    st.dataframe(query_data_5.set_index(query_data_5.columns[0]),width=1200)
    # Note it might be of interest to add spread filtering and sorting as well.

# Table showing current risky countries in the system. 
st.sidebar.write('Risky Countries Settings')
st.sidebar.dataframe(query_data_2.set_index(query_data_2.columns[0]))


                   
