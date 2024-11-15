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
# Global counter for tracking updates
index_counter = 0

def index_node_health(key, value):
    """
    Index a node_health entry into Elasticsearch.
    """
    global index_counter
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
        index_counter += 1
        print(f"[{index_counter}] Indexed node_health for node_id: {node_id}, result: {response['result']}")
    
    except Exception as e:
        print(f"Error indexing node_health for key {key}: {e}")

# Function to process all existing node:health keys on startup
def process_existing_node_health_keys():
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match="node:health:*")
        for key in keys:
            value = redis_client.get(key)
            if value:
                index_node_health(key, value)
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
            value = redis_client.get(key);
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
    # Process existing keys on startup
    process_existing_node_health_keys()

    # Listen for new keys
    listen_for_node_health_updates()

    #redis keyspace notifications: redis-cli config set notify-keyspace-events KEA