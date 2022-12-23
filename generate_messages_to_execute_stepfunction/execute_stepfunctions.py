import json
import boto3
from datetime import datetime


def lambda_handler(event, context):
    message_body = event['Records'][0]['body']
    print("Event read: "+message_body)
    
    json_acceptable_string = message_body.replace("'", "\"")
    message_dict = json.loads(json_acceptable_string)
    print(message_dict)
    iteration= str(message_dict['Iteration'])
    id= str(message_dict['ID'])
    print("Processing ID:  "+id)
 
    
    date_time_obj = datetime.now().strftime("%d-%b-%Y-(%H-%M-%S-%f)")
    state_name = 'Execution-ID-'+id+'-'+str(date_time_obj)
    
    client = boto3.client('stepfunctions')
    response = client.start_execution(
        stateMachineArn='arn:aws:states:eu-central-1:<account-id>:stateMachine:SF-POC-Role-MB',
        name=state_name,
        input=message_body
    )
    print("State Machine Started: "+ state_name)
