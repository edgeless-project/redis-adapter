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
    ['elasticseach_url:9200'],
    basic_auth=('test', 'test'),
    verify_certs=False,
    ssl_show_warn=False
)


def index_to_elasticsearch(key, value):
    global index_counter
    # Print the Redis contents
    key_type = redis_client.type(key).decode()
    print(f"Key: {key.decode()}, Type: {key_type}")
    key_parts = key.decode().split(':')
    index = key_parts[0]  # index type: node, provider, dependency, instance, function, performance, workflow
    timestamp = datetime.now().isoformat()

    # Default doc to None
    doc = None

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
    
    if index == "function":
        function_id = key_parts[1]
        
        if key_parts[2] == "average":
            doc = {'function_id': function_id, 'average': float(value.decode()), 'timestamp': timestamp}
        
        elif key_parts[2] == "samples":
            doc = {
                'function_id': function_id,
                'samples': doc,
                'timestamp': timestamp
            }
        index_name = "tid_function"

    elif index == "node":
        node_metric_type = key_parts[1]
        node_id = key_parts[2]
        doc['timestamp'] = timestamp
        doc['node_id'] = node_id

        if node_metric_type == "capabilities":
            index_name = "tid_node_capabilities"
        elif node_metric_type == "health":
            index_name = "tid_node_health"
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
        index_name = "tid_provider"
    
    elif index == "dependency":
        doc['timestamp'] = timestamp
        doc['dependency_id'] = key_parts[1]
        index_name = "tid_dependency"
        
    elif index == "instance":
        doc['timestamp'] = timestamp
        instance_id = key_parts[1]
        doc['instance_id'] = instance_id
        
        if 'Resource' in doc:
            normalized_resource = []
            for item in doc['Resource']:
                if isinstance(item, dict):
                    normalized_resource.append(item)
                else:
                    normalized_resource.append({"InstanceId": item})
                doc['Resource'] = normalized_resource
        
        if 'Function' in doc:
            normalized_function = []
            for item in doc['Function']:
                if isinstance(item, dict):
                    normalized_function.append(item)
                else:
                    normalized_function.append({"InstanceId": item})
                doc['Function'] = normalized_function
        
        index_name = "tid_instance"

    elif index == "performance":
        function_id = key_parts[2]
        doc = {
            'function_id': function_id,
            'execution_times': doc,
            'timestamp': timestamp
        }
        index_name = "tid_performance"
    
    elif index == "workflow":
        if not isinstance(doc, dict):
            doc = {}
        workflow_metric_type = key_parts[1]
        doc['workflow_type'] = workflow_metric_type

        if key_parts[2] == "average":
            doc['average'] = float(value.decode())

        elif key_parts[2] == "samples":
            samples = value.decode().split()
            doc['samples'] = [
                {"value": float(sample.split(',')[0]), "timestamp": float(sample.split(',')[1])}
                for sample in samples
            ]
        index_name = "tid_workflow"
    else:
        print(f"Skipping key: {key.decode()} due to unrecognized index")
        return
    
    # forward into Elasticsearch
    try:
        index_counter = index_counter + 1
        res = es.index(index=index_name, body=doc)
        print(f"Indexed document: {index_counter})  {res['result']}")

    except Exception as e:
        print(f"Error indexing document: {e}")
