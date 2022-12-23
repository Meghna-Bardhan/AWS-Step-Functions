import json
import datetime

def lambda_handler(event, context):
    
    print("Event Received in 1st Lambda: ")
    print(event)
    
    response = {}
    response['OrderType'] = event['OrderType'] + "1"
    response['Timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    response['Message'] = 'Hello From 1st Lambda inside Step Function'
    print(response)
    
    return response

