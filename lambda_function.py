import json
import boto3
import io
import pandas as pd 

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn  = "arn:aws:sns:us-east-1:058264373160:doordash-notification-alert"

def lambda_handler(event, context):
    
    print(event)
    try:
        s3_bucket_name = event['Records'][0]['s3']['bucket']['name']
        s3_object_key = event['Records'][0]['s3']['object']['key']
        print(s3_bucket_name)
        print(s3_object_key)
        resp = s3_client.get_object(Bucket = s3_bucket_name, Key = s3_object_key)
        print(resp["Body"])
        s3_object_body = resp["Body"].read().encode('utf-8')
        print(s3_object_body)
        delivery_records = json.loads(s3_object_body)
        print("1", delivery_records)  # Parse JSON array into list of dictionaries

        df_s3_bucket = pd.DataFrame(delivery_records)  # Create DataFrame from list of dictionaries
        print(df_s3_bucket.head())

        df_filtered = df_s3_bucket[df_s3_bucket["status"] == "delivered"]
        print("2",df_filtered.head()) # my business requirement is that I have to filter records on the basis of status

        csv_buffer = io.StringIO()
        df_filtered.to_csv(csv_buffer, index=False)


        s3_upload_bucket = "doordash-landing-s3-bucket"
        s3_upload_object_key = "file.txt"
        s3_client.put_object(Bucket = s3_upload_bucket, Key = s3_upload_object_key, Body = csv_buffer.getvalue())

        message = "Successfully filtered delivery status from your file 😁 : {} ".format("s3://" + s3_bucket_name + "/" + s3_object_key)
        sns_client.publish(Subject = "SUCCESS - Delivery Status filtered", TargetArn = sns_arn, Message = message, MessageStructure = 'text')

    except Exception as err:
        message = "Unable to filter delivery status from your file 😢 : {} ".format("s3://" + s3_bucket_name + "/" + s3_object_key)
        sns_client.publish(Subject = "FAILED - Delivery Status not filtered", TargetArn = sns_arn, Message = message, MessageStructure = 'text')


