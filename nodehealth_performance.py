import redis
from elasticsearch import Elasticsearch
import json
from datetime import datetime

# Connect to Redis
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)

# Connect to Elasticsearch
es = Elasticsearch(
    ['es_url'],
    basic_auth=('test', 'test'),
    verify_certs=False,
    ssl_show_warn=False
)
# Global counters
index_counter_node_health = 0
index_counter_performance = 0

def index_node_health(key, value):
    """
    Index a node_health entry into Elasticsearch.
    """
    global index_counter_node_health
    try:
        # Decode and parse the Redis key and value
        key_str = key.decode()
        value_str = value.decode()
        node_id = key_str.split(':')[-1]  # Extract node ID from key
        doc = json.loads(value_str)
        
        # Add additional metadata
        doc['timestamp'] = datetime.now().isoformat()
        doc['node_id'] = node_id

        # Index the document into Elasticsearch
        index_name = "node_health"
        response = es.index(index=index_name, body=doc)
        index_counter_node_health += 1
        print(f"[{index_counter_node_health}] Indexed node_health for node_id: {node_id}, result: {response['result']}")
    
    except Exception as e:
        print(f"Error indexing node_health for key {key}: {e}")

def index_performance_to_elasticsearch(key, value):
    """
    Index performance keys from Redis to Elasticsearch.
    """
    global index_counter_performance
    try:
        key_parts = key.decode().split(':')
        if key_parts[0] != "performance":
            print(f"Skipping non-performance key: {key.decode()}")
            return

        # Parse Redis value (expected list of execution times and timestamps)
        doc = [
            {"execution_time": float(item.split(',')[0]), "timestamp": float(item.split(',')[1])}
            for item in value.decode().split()
        ]

        function_id = key_parts[2]  # Extract function ID
        timestamp = datetime.now().isoformat()

        # Document structure
        document = {
            'function_id': function_id,
            'execution_times': doc,
            'timestamp': timestamp
        }

        # Index name
        index_name = "performance"

        # Forward to Elasticsearch
        response = es.index(index=index_name, body=document)
        index_counter_performance += 1
        print(f"[{index_counter_performance}] Indexed document into '{index_name}', result: {response['result']}")

    except Exception as e:
        print(f"Error indexing key {key.decode()} to Elasticsearch: {e}")

# def process_existing_node_health_keys():
#     """
#     Process all existing node:health keys in Redis and index them into Elasticsearch.
#     """
#     cursor = 0
#     while True:
#         cursor, keys = redis_client.scan(cursor=cursor, match="node:health:*")
#         for key in keys:
#             value = redis_client.get(key)
#             if value:
#                 index_node_health(key, value)
#         if cursor == 0:
#             break

def process_existing_performance_keys():
    """
    Process all existing performance keys in Redis and index them into Elasticsearch.
    """
    print("Processing existing performance keys...")
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match="performance:*")
        for key in keys:
            value = redis_client.get(key)
            if value:
                index_performance_to_elasticsearch(key, value)
        if cursor == 0:
            break

def listen_for_node_health_updates():
    """
    Listen for updates to node:health keys in Redis and index them into Elasticsearch.
    """
    pubsub = redis_client.pubsub()
    pubsub.psubscribe('__key*__:*')

    print("Listening for node:health updates...")

    for message in pubsub.listen():
        if message['type'] == 'pmessage':
            key = message['data']
            # Decode the key and filter for node:health entries
            key_str = key.decode()
            if key_str.startswith("node:health:"):
                value = redis_client.get(key)
                if value:
                    print(f"Processing node:health key: {key_str}")
                    index_node_health(key, value)
                else:
                    print(f"Received node:health key: {key_str}, but no value found in Redis.")
            else:
                print(f"Ignored key: {key_str}")

if __name__ == "__main__":
    # Process existing node_health keys on startup
    # process_existing_node_health_keys()

    # Listen for new node_health keys
    from threading import Thread
    listener_thread = Thread(target=listen_for_node_health_updates)
    listener_thread.start()

    # Process performance keys at the end
    try:
        listener_thread.join()  # Wait for listener to finish
    except KeyboardInterrupt:
        print("\nStopping listener and processing performance keys...")
        process_existing_performance_keys()
        print("Performance keys processing complete.")


    #redis keyspace notifications: redis-cli config set notify-keyspace-events KEA