import json
import boto3
import os
import urllib.request
import logging

sqs = boto3.client('sqs')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

#set Log Level to Info for trouble shooting
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Using the extracted S3 object information from SNS message, download the S3 object from aws earth s3 url 
def download_data_object(bucket, key, folder):
       os.chdir(os.environ['tempfolder'])
       url = os.environ['awsearthurl'] + bucket + "/" + key
       filename = os.environ['tempfolder'] + key
       urllib.request.urlretrieve(url, filename)
       
       with open(filename, 'rb') as f:
              s3_client.upload_fileobj(f, os.environ['uploadbucket']
                                            , os.environ['bucketfolder'] +folder+'/'+key) # save in this directory with same name
    

def lambda_handler(event, context):
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)
    try:
        for record in event['Records']:
                  notification = record['Body'] #extract the SNS notification which has been stored in the Body attribute
                  message = notification['Message'] #information about the S3 object which has been stored in the notification Message
                  bucket = message['bucket'] # get bucket name from message dictionary
                  key = message['key'] #get key from message dictionary
                  name = message['name'] #get parameter name from message dictionary
                  
                  download_data_object(bucket, key, name)
        return {
            "statusCode": 200,
            "body": json.dumps('aws earth data file download sucessful')
        }
    except Exception:
        raise 
    
