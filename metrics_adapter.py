#This script reads metrics from a RedisDB and forwards them to an Elasticsearch Endpoint

# Required Libraries
import redis
from elasticsearch import Elasticsearch, helpers
import json
from datetime import datetime, timezone

#Current setup for local Redis and remote Elasticsearch

# Connect to Redis
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)

# Connect to Elasticsearch
es = Elasticsearch(
    ['es_url'],
    basic_auth=('test', 'test'),
    verify_certs=False,
    ssl_show_warn=False
)

#current support indexes are: node, provider, dependency, instance, function, performance, workflow


index_counter = 0
#index Redis entries into Elasticsearch
def index_to_elasticsearch(key, value):
    global index_counter
    # Print the Redis contents
    key_type = redis_client.type(key).decode()
    print(f"Key: {key.decode()}, Type: {key_type}")
    # print(f"Value: {value.decode()}")
    key_parts = key.decode().split(':')
    index = key_parts[0]  # index type: node, provider, dependency, instance, function, performance, workflow
    timestamp = datetime.now().isoformat()

    # Default doc to None
    doc = None
    # handle the case where value is a float (for lists type keys)
    if key_type == "string":
    # Handle the case where the value is a string (JSON or float)
        try:
            doc = json.loads(value.decode())
        except json.JSONDecodeError:
            print(f"Value is not a JSON object: {value.decode()}")
            try:
                doc = float(value.decode())
            except ValueError:
                print(f"Value is not a float: {value.decode()}")
                return

    elif key_type == "list":
        # Handle lists (for example, function samples)
        values = redis_client.lrange(key, 0, -1)  # Get the entire list
        doc = [
            {"value": float(item.decode().split(',')[0]), "timestamp": float(item.decode().split(',')[1])}
            for item in values
        ]
    # index based on the index type
    if index == "function":
        function_id = key_parts[1]
        
        # Check if key is for 'average' or 'samples'
        if key_parts[2] == "average":
            # Since it's an average, the value is a float
            doc = {'function_id': function_id, 'average': float(value.decode()), 'timestamp': timestamp}
        
        elif key_parts[2] == "samples":
            # Handle 'samples' as a list of floats and timestamps
            doc = {
                'function_id': function_id,
                'samples': doc,  # the list we created from the 'lrange'
                'timestamp': timestamp
            }
        index_name = "function"

    elif index == "node":
        node_metric_type = key_parts[1]  # "capabilities" or "health"
        node_id = key_parts[2]
        doc['timestamp'] = timestamp
        doc['node_id'] = node_id

        if node_metric_type == "capabilities":
            index_name = "node_capabilities"
        elif node_metric_type == "health":
            index_name = "node_health"
        else:
            print(f"Unrecognized node metric type for key: {key.decode()}")
            return
    elif index == "provider":
        doc['timestamp'] = timestamp
        node_id = doc.get('node_id')
        if node_id is None:
            print(f"Skipping key: {key} due to missing node_id in provider entry")
            return
        doc['node_id'] = node_id
        index_name = "provider"
    
    elif index == "dependency":
        doc['timestamp'] = timestamp
        doc['dependency_id'] = key_parts[1]
        index_name = "dependency"
        
    elif index == "instance":
        doc['timestamp'] = timestamp
        instance_id = key_parts[1]
        doc['instance_id'] = instance_id
        # Normalize Resource field if it exists
        if 'Resource' in doc:
            normalized_resource = []
            for item in doc['Resource']:
                if isinstance(item, dict):
                    normalized_resource.append(item)
                else:
                    normalized_resource.append({"InstanceId": item})
                doc['Resource'] = normalized_resource
        
        # Normalize Function field if it exists
        if 'Function' in doc:
            normalized_function = []
            for item in doc['Function']:
                if isinstance(item, dict):
                    normalized_function.append(item)
                else:
                    normalized_function.append({"InstanceId": item})
                doc['Function'] = normalized_function
        
        index_name = "instance"

    elif index == "performance":
        function_id = key_parts[2]
        # Handle performance data (list of execution times and timestamps)
        doc = {
            'function_id': function_id,
            'execution_times': doc,  # List of parsed performance data
            'timestamp': timestamp
        }
        index_name = "performance"
    
    elif index == "workflow":
        #convert to dict if not
        if not isinstance(doc, dict):
            doc = {}
        workflow_metric_type = key_parts[1]  # e.g., vector_mul_wf_chain
        doc['workflow_type'] = workflow_metric_type

        if key_parts[2] == "average":
            # Handle average value
            doc['average'] = float(value.decode())
            

        elif key_parts[2] == "samples":
            # Handle workflow samples
            samples = value.decode().split()
            doc['samples'] = [
                {"value": float(sample.split(',')[0]), "timestamp": float(sample.split(',')[1])}
                for sample in samples
            ]
        index_name = "workflow"
    else:
        print(f"Skipping key: {key.decode()} due to unrecognized index")
        return
    
    # print(f"Indexing into {index_name}: {json.dumps(doc, indent=2)}")
    
    # forward into Elasticsearch
    try:
        index_counter = index_counter + 1
        res = es.index(index=index_name, body=doc)
        print(f"Indexed document: {index_counter})  {res['result']}")


        # print(f"Total indexed documents: {index_counter}")
    except Exception as e:
        print(f"Error indexing document: {e}")

# on the start of the script go to redis and fetch all existing entries
def index_existing_entries():
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor)
        for key in keys:
            key_type = redis_client.type(key).decode()

            # Handle only string keys (type = "string")
            if key_type == "string":
                value = redis_client.get(key)
                if value:
                    index_to_elasticsearch(key, value)
            elif key_type == "list":
                values = redis_client.lrange(key, 0, -1)
                if values:
                    for value in values:
                        index_to_elasticsearch(key, value)
                # print(f"Skipping key: {key.decode()} due to incompatible type: {key_type}")
            else:
                print(f"Skipping key: {key.decode()} due to incompatible type: {key_type}")
        
        if cursor == 0:
            break

# keep listening on redis channel for new entries
def listen_for_new_entries(redis_channel):
    pubsub = redis_client.pubsub()
    pubsub.psubscribe('__key*__:*')

    print(f"Subscribed to Redis channel: {redis_channel}")

    for message in pubsub.listen():
        if message['type'] == 'pmessage':
            key = message['data']
            value = redis_client.get(key)
            
            if value:
                print(f"received message for key: {key} {value}")
            else:
                print(f"Received message for key: {key}, but no value found in Redis")


# Index all existing entries on script startup
index_existing_entries()

# Subscribe to listen for new entries
if __name__ == "__main__":
    redis_channel = '__key*__:*'  # Specify the Redis channel to subscribe to
    listen_for_new_entries(redis_channel)