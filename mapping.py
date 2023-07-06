# Heineken - ETL - Test Code

This notebook is the test code for an ETL process for Heineken. The process extracts a json file from AWS, maps the data in the file to a new structure, creating new csv files, and uploads those new files to a different location in AWS.

The transformation or mapping is done according to Heineken's global EDM and involves the following 4 sections:

- Consumer
- Online Engagement
- Consent Event
- Affinity

Each of these tables has their own structure/schema which is documented. Ask Marco Ghibaudi for access.
## Imports
%idle_timeout 2880
%glue_version 3.0
%worker_type G.1X
%number_of_workers 5

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

import boto3
import botocore
import hashlib
import json
import typing
import pandas as pd

from datetime import datetime, date
## Statics
CLIENT = boto3.client('s3')
S3_INPUT_BUCKET = 'hbr-input-test'
S3_OUTPUT_BUCKET = 'hbr-output-test'

S3KeyType = typing.NewType('S3KeyType', typing.Dict[str, typing.Union[str, int, datetime]])
## Functions
def list_keys(client: botocore.client.BaseClient, bucket: str, prefix: str='')->typing.List[S3KeyType]:
    """Gets a list of all non-empty keys from the specified S3 location."""
    
    paginator = client.get_paginator('list_objects_v2')

    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    valid_keys = []
    
    for page in pages:
        for key in page.get("Contents", []):
            if key["Size"] > 0:
                valid_keys.append(key)

    return valid_keys
    
## Extract JSON
keys = list_keys(CLIENT, S3_INPUT_BUCKET)
all_entries = [
    json.load(CLIENT.get_object(Bucket=S3_INPUT_BUCKET, Key=key["Key"])["Body"])
    for key in keys
]
## Mappings
# Note that this test entry is clean.
# There will likely need to be some data cleaning work for all data.
def map_country_to_code(country: str)->str:
    """Maps country name to code if country name is Brasil/Brazil"""
    
    return 'BR' if country.lower() in ['brasil', 'brazil'] else ''


def get_consent_status(consent:str)->str:
    """Reformats the consent status text"""
    
    return 'Opt_in' if consent=='True' else 'Opt_out'
    
def get_online_engagement_key(consumer_key: str, online_engagement_date: str, url: str)->str:
    """Returns an online engagment key as per client specs - a hash of the arguments."""
    
    field_string = f"{consumer_key}-{online_engagement_date}-{url}"
    
    online_engagement_key = hashlib.sha256(field_string.encode()).hexdigest()
    
    return online_engagement_key


def get_affinity_key(consumer_key: str, affinity_cat: str, affinity_subcat: str)->str:
    """Returns an affinity key as per client specs - a hash of the arguments."""
    
    field_string = f"{consumer_key}-{affinity_cat}-{affinity_subcat}"
    
    affinity_key = hashlib.sha256(field_string.encode()).hexdigest()
    
    return affinity_key


def get_consent_event_key(
    consumer_key: str, 
    consent_date: str, 
    consent_status: str, 
    opco: str, 
    sub_level: str
)->str:
    """Returns a consent event key as per client specs - a hash of the arguments."""
    
    field_string = f"{consumer_key}-{consent_date}-{consent_status}-{opco}-{sub_level}"
    
    consent_event_key = hashlib.sha256(field_string.encode()).hexdigest()
    
    return consent_event_key

consumer_entries = []
online_engagement_entries = []
affinity_entries = []
consent_entries = []

for entry in all_entries:
    
    email_hash = entry['hashed_information']['email_hash']
    last_modified_date = datetime.strptime(entry['update_date'], "%Y-%m-%d %H:%M:%S.%f")

    new_consumer_entry = {
        "Consumer_Key": email_hash,
        "Deletion_Flag": False,
        "Email_Address": entry['email'],
        "Salutation": '',
        "First_Name": entry['name'],
        "Middle_Name": '',
        "Last_Name": entry['surname'],
        "Suffix": '',
        "Gender": entry['gender'],
        "Birthdate": entry['birth_date'],
        "Nationality": '',
        "Phone_Number": entry['mobile_phone_fb'],
        "Address_Line_1": entry['address'],
        "Address_Line_2": entry['district'],
        "Postal_Code": entry['zipcode'],
        "City": entry['city'],
        "Region_Key": entry['state'],
        "Country_ISO_2": map_country_to_code(entry['country']),
    }
    
    consumer_entries.append(new_consumer_entry)
    
    new_consent_entry = {
        "Consumer_Key": email_hash,
        "Consent_Event_Key": get_consent_event_key(
            consumer_key=email_hash,
            consent_date=datetime.strftime(last_modified_date, "%Y-%m-%dT%H:%M:%S"),
            consent_status=get_consent_status(entry['media_consent']),
            opco='BR001',
            sub_level=''
        ),
        "Data_Collection_Type": 'First Party',
        "Consent_Sub_Level": '',
        "OpCo": 'BR001OC',
        "Legal_Text": '',
        "Consent_Date": datetime.strftime(last_modified_date, "%Y-%m-%dT%H:%M:%S"),
        "Data_Use_Purpose": 'Marketing',
        "Consent_Status": get_consent_status(entry['media_consent']),
        "Data_Use_Channel": 'Omnichannel'
    }
    
    consent_entries.append(new_consent_entry)
    
    for source in entry['sources']:
        
        reg_date = datetime.strptime(source['reg_date'], "%Y-%m-%d %H:%M:%S.%f")
        
        new_online_entry = {
            "Online_Engagement_Type": source['source_category'],
            "Platform_Name": '',
            "Url": 'www.unknown.com',
            "Referring_Url": '',
            "Page_Name": source['source_title'],
            "Operating_System": '',
            "Consumer_Key": email_hash,
            "Campaign_Key": f"{reg_date.year}_{source['source_title']}",
            "Online_Engagement_Date": datetime.strftime(reg_date, "%Y-%m-%dT%H:%M:%S"),
            "Engagement_Channel": 'Online',
            "Asset_Key": '',
            "Brand_Key": '',
            "Product_Key": '',
            "Online_Engagement_Key": get_online_engagement_key(
                consumer_key=email_hash, 
                online_engagement_date=datetime.strftime(reg_date, "%Y-%m-%dT%H:%M:%S"),
                url='www.unknown.com'
            )
        }
    
        online_engagement_entries.append(new_online_entry)
    
    for behaviour in entry['behaviour']:
        
        if behaviour['type'] == 'brand_interest':
        
            new_affinity_entry = {
                "Consumer_Key": email_hash,
                "Affinity_Key": get_affinity_key(
                    consumer_key=email_hash,
                    affinity_cat="ALCOHOLIC_BEER",
                    affinity_subcat=behaviour['value'].upper()
                ),
                "Affinity_Category": "ALCOHOLIC_BEER",
                "Affinity_Subcategory": behaviour['value'].upper(),
                "Affinity_Value": behaviour['value'].upper(),
                "Affinity_Type": 'declared',
                "Affinity_Score": '10',
                "Affinity_Last_Modified_Date": '',
            }
            
            affinity_entries.append(new_affinity_entry)
            
## Load to S3 Output
pd.DataFrame(consumer_entries).to_csv(
    f's3://{S3_OUTPUT_BUCKET}/{date.today()}_hbr_consumer_test.csv', 
    index=False
)
pd.DataFrame(online_engagement_entries).to_csv(
    f's3://{S3_OUTPUT_BUCKET}/{date.today()}_hbr_online_engagement_test.csv', 
    index=False
)
pd.DataFrame(consent_entries).to_csv(
    f's3://{S3_OUTPUT_BUCKET}/{date.today()}_hbr_consent_event_test.csv', 
    index=False
)
pd.DataFrame(affinity_entries).to_csv(
    f's3://{S3_OUTPUT_BUCKET}/{date.today()}_hbr_affinity_test.csv', 
    index=False
)
