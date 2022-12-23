import json
import threading
import boto3
from datetime import datetime

class ActionableThread(threading.Thread):
    def __init__(self, thread_name, action, action_args):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.action = action
        self.action_args = action_args
        
    def run(self):
        print("Starting New Thread: {}".format(self.thread_name))
        #print("The ARGS: ", self.action_args)
        self.action(*self.action_args)
        print("Thread {} DONE".format(self.thread_name))

def generate_json(number, thread_num):
    #print("THE Stuff here: {}, {}".format(number, thread_num))
    thread_name = str(thread_num+1)
    
    for i in range(int(number)):
        date_time_obj = datetime.now().strftime("%d-%b-%Y-%H-%M-%S-%f")
        id = "SCAL-MB-T"+thread_name+"-N"+str(i+1)
        group_id = id +"-D"+ str(date_time_obj)
        per_order =	{
            "OrderType": "BUY-T"+thread_name+"-N"+str(i+1),
            "ID": id,
            "Iteration": str(i+1)
        }
        json_object = json.dumps(per_order)
        print(json_object)
        send_message_to_sqs(json_object, group_id)
        
def send_message_to_sqs(message, group_id):
    print("Sending message to SQS")
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl="https://sqs.eu-central-1.amazonaws.com/<account-id>/SF-SQS-FIFO-POC-MB.fifo",
        MessageBody=message,
        MessageGroupId=group_id,
        MessageDeduplicationId="dedup"+group_id
    )
    print("Message of group id "+group_id+" sent to SQS")


def lambda_handler(event, context):
    # Start threading logic here.....
    
    num_threads = 10
    all_threads = []
    for i in range(num_threads):
        all_threads.append( ActionableThread(
            "T-{}".format(i), 
            generate_json, # function call
            (event['Number'], i) # args for function call
        ) )
        
    for i in range(num_threads):
        all_threads[i].start()
        print("Thead{} Running".format(i))
        all_threads[i].join()
        
    print("All Thread Run Over")
    
    return {
        'statusCode': 200,
        'body': "Messages SENT"
    }
