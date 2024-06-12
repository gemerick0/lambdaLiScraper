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
    f = open(s3.download_file(os.environ['BUCKET'], os.environ['AUDIENCE_ID'] + '.json', os.environ['AUDIENCE_ID']) + '.json', 'r')
    lk_credentials = json.load(f)
    f.close()
    if os.environ['SCRAPE']:
        driver = WebDriverSalesNavScraper(os.environ['AUDIENCE_ID'], os.environ['PROXYID'])
        logger.info('## SCRAPING')
        driver.run()

    else:
        driver = WebDriverProfileScraper(os.environ['AUDIENCE_ID'], os.environ['PROXYID'])
        logger.info('## ENRICHING')



    driver.close()

    if all (k in os.environ for k in ('BUCKET','DESTPATH')):
        ## Upload generated screenshot files to S3 bucket.
        s3.upload_file('/tmp/{}-fixed.png'.format(screenshot_file),
                    os.environ['BUCKET'],
                    '{}/{}-fixed.png'.format(os.environ['DESTPATH'], screenshot_file))
        s3.upload_file('/tmp/{}-full.png'.format(screenshot_file),
                    os.environ['BUCKET'],
                    '{}/{}-full.png'.format(os.environ['DESTPATH'], screenshot_file))
