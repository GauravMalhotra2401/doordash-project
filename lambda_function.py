import json
import boto3
import io
import pandas as pd 

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = "arn:aws:sns:us-east-1:058264373160:doordash-notification-alert"

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            s3_bucket_name = record['s3']['bucket']['name']
            s3_object_key = record['s3']['object']['key']
            
            # Retrieve the object from S3
            obj = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
            lines = obj['Body'].read().decode('utf-8').splitlines()
            
            # Initialize an empty list to store filtered records
            filtered_records = []
            
            # Process each line as a JSON object
            for line in lines:
                try:
                    # Parse JSON from each line
                    data = json.loads(line)
                    # Assuming data is a dictionary, process it accordingly
                    if data.get("status") == "delivered":
                        # Add the record to the list of filtered records
                        filtered_records.append(data)
                except Exception as e:
                    # Handle JSON parsing errors
                    print("Error parsing JSON:", e)
                    continue  # Skip to the next line if parsing fails
            
            # Create a DataFrame from the filtered records
            df_filtered = pd.DataFrame(filtered_records)
            
            # Convert DataFrame to CSV format
            csv_buffer = io.StringIO()
            df_filtered.to_csv(csv_buffer, index=False)
            
            # Upload CSV to S3
            s3_upload_bucket = "doordash-landing-s3-bucket"
            s3_upload_object_key = "file.txt"
            s3_client.put_object(Bucket=s3_upload_bucket, Key=s3_upload_object_key, Body=csv_buffer.getvalue())
            
            # Example: Publish a message to SNS for successful processing
            sns_client.publish(
                Subject="SUCCESS - Data Processing",
                TargetArn=sns_arn,
                Message="Successfully processed data from S3 object: s3://{}/{}".format(s3_bucket_name, s3_object_key),
                MessageStructure='text'
            )
    
    except Exception as e:
        # Handle exceptions
        print("Error:", e)
        sns_client.publish(
            Subject="ERROR - Data Processing",
            TargetArn=sns_arn,
            Message="Error processing data from S3 object: s3://{}/{}".format(s3_bucket_name, s3_object_key),
            MessageStructure='text'
        )
