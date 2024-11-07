Redis to Elasticsearch Metrics Forwarder for TID Testbed Integration
This script reads metrics from a Redis database and forwards them to specified indices in an Elasticsearch cluster. It supports various metric types, including node, provider, dependency, instance, function, performance, and workflow metrics.

Prerequisites
Python 3.7+
Redis server: Ensure Redis is running locally on 127.0.0.1:6379
Elasticsearch server: Ensure Elasticsearch is running and accessible at the specified endpoint
Dependencies
Install the required libraries using pip:

bash
Copy code
pip install redis elasticsearch datetime
Configuration
Update the script if necessary to match your Redis and Elasticsearch configurations:

Redis is expected to be running locally on 127.0.0.1:6379
Elasticsearch endpoint url and credentials are provided to TID. If any partner wishes to try the adapter please contact me (Panagiotis Antoniou - AEGIS) to provide you with the credentials.  
You can change the following parameters directly in the script to suit your environment:

python
Copy code
# Connect to Redis
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)

# Connect to Elasticsearch
es = Elasticsearch(
    ['elasticseach_url:9200'],
    basic_auth=('test', 'test'), 
    verify_certs=False,
    ssl_show_warn=False
)
Running the Script
Initial Indexing of Existing Entries
The script starts by indexing all existing entries from Redis to Elasticsearch. This is done using the index_existing_entries function.

Listening for New Entries
After indexing existing entries, the script continuously listens for new entries in Redis using a pub/sub channel. It will automatically forward new entries to the corresponding Elasticsearch indices.

To run the script:

bash
Copy code
python redis_to_elasticsearch_forwarder.py
Supported Indexes


tid_node
tid_provider
tid_dependency
tid_instance
tid_function
tid_performance
tid_workflow
Each index corresponds to specific metrics defined in Redis, and the script maps Redis keys to these indices based on predefined key patterns.

Additional Information
Redis Key Types: This script processes Redis keys of type string and list. Other types will be ignored.
Debugging: The script prints debug information to the console, including the indexed document count and any errors encountered during indexing.
Troubleshooting
If you encounter connection errors:

Check that both Redis and Elasticsearch are running and accessible.
Verify your Redis and Elasticsearch configurations in the script.
License
This script is provided under the MIT License.