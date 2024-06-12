import boto3
import os
import logging
import json
from liScraper import WebDriverProfileScraper, WebDriverSalesNavScraper

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    
    # Download the JSON file from the specified S3 bucket
    bucket_name = os.environ['BUCKET']
    file_key = os.environ['AUDIENCE_ID'] + '.json'
    local_file_path = '/opt/bin/tmp/' + file_key  # Save the file in /tmp directory
    s3.download_file(bucket_name, file_key, local_file_path)
    
    # Read the JSON file
    with open(local_file_path, 'r') as file:
        lk_credentials = json.load(file)
    
    if os.environ['SCRAPE']:
        driver = WebDriverSalesNavScraper(os.environ['AUDIENCE_ID'], os.environ['PROXYID'])
        logger.info('## SCRAPING')
        cookies = driver.run()
        lk_credentials['cookies'] = cookies['li_at']
    else:
        driver = WebDriverProfileScraper(os.environ['AUDIENCE_ID'], os.environ['PROXYID'])
        logger.info('## ENRICHING')
        cookies = driver.scrape()
        lk_credentials['cookies'] = cookies['li_at']

    # Update the JSON content
    updated_json = json.dumps(lk_credentials)
    
    # Upload the updated JSON file back to the S3 bucket
    s3.upload_file(local_file_path, bucket_name, file_key)
    
    driver.close()

    