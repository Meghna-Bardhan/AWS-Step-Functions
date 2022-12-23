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
        print("The ARGS: ", self.action_args)
        self.action(*self.action_args)
        print("Thread {} DONE".format(self.thread_name))

def generate_json(number, thread_num):
    print("THE Stuff here: {}, {}".format(number, thread_num))
    for i in range(int(number)):
        per_order =	{
            "OrderType": "SELL-T"+str(thread_num)+"-N"+str(i+1),
            "ID": "SCAL-MB-T"+str(thread_num)+"-O"+str(i+1),
            "Iteration": str(i+1)
        }
        json_object = json.dumps(per_order)
        print(json_object)
        start_triggering_state_machine(json_object, str(thread_num), str(i+1))
        
def start_triggering_state_machine(message_body, thread_num, count):
    print("Calling the start_triggering_state_machine")
    date_time_obj = datetime.now().strftime("%d-%b-%Y-(%H-%M-%S-%f)")
    state_name = 'Execution-T'+thread_num+'-N'+count+'-'+str(date_time_obj)
    boto3_session = boto3.session.Session()
    print("BOTO3 Sesion", boto3_session)
    client = boto3_session.client('stepfunctions')
    print("Client Stuff", )
    response = client.start_execution(
        stateMachineArn='arn:aws:states:eu-central-1:<account-id>:stateMachine:SF-POC-Role-MB',
        name=state_name,
        input=message_body
    )
    print("State Machine Started: "+state_name)


def lambda_handler(event, context):
    # Start threading logic here.....
    
    num_threads = 3
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
