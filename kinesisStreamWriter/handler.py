import boto3
import gzip
import os
import tempfile
import traceback
import urllib
import uuid

# Load the S3 client for reading data from S3 when notified of creation.
s3 = boto3.client('s3')

# Load the Kinesis client for putting log entries into the Kinesis stream.
kinesis = boto3.client('kinesis')

# Get the name of the Kinesis Stream to write to from environment variable.
kinesis_stream_name = os.environ.get('KINESIS_STREAM_NAME')

# The 'event' here will be an S3 ObjectCreated:* event.
def stream(event, context):
    # Get the name of the bucked the object was created in.
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Get the full path to the log file created relative to the bucket.
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    try:
        # Get the metadata for the log file created, including a streaming
        # object for the contents of the file.
        resp = s3.get_object(Bucket=bucket, Key=key)
        
        # Read the contents of the file from the streaming object.
        data = resp['Body'].read()
        
        # Decompress the data if the file name (key) has a .gz extension
        if key[-3:] == '.gz':
            with tempfile.TemporaryFile() as temp_file:
                temp_file.write(data)
                temp_file.seek(0)
                
                with gzip.GzipFile(fileobj=temp_file, mode='r') as gz:
                    data = gz.read()
        
        # Split the contents of the log file into individual log entries.
        lines = data.splitlines()
        
        for line in lines:
            # Put each log entry into the Kinesis Stream.
            kinesis.put_record(StreamName=kinesis_stream_name, Data=line, PartitionKey=str(uuid.uuid1()))
    except Exception as e:
        print('Exception: {}'.format(e))
        traceback.print_exc()
