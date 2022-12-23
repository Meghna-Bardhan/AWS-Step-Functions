import json
import boto3

def lambda_handler(event, context):
    
    for i in range(event['Number']):
        per_order =	{
            "OrderType": "SELL"+str(i+1),
            "ID": "SCAL-MB-X"+str(i+1),
            "Iteration": str(i+1)
        }
        print(per_order)
        json_object = json.dumps(per_order)
        sqs = boto3.client('sqs')
        sqs.send_message(
            QueueUrl="https://sqs.eu-central-1.amazonaws.com/<account-no>/SF-SQS-POC-MB",
            MessageBody=json_object
        )
    
    
    return {
        'statusCode': 200,
        'body': "Messages SENT"
    }
