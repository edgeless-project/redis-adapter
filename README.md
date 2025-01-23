

# Redis Adapter

## Requirements
At least one ε-ORC Node with Redis Proxy enabled.  
A host machine with Python 3+ installed.  


```bash
sudo apt install python
pip install redis elasticsearch datetime
```
Enable Redis Notifications for continuous updates.  
At the host where Redis is running:
```
redis-cli config set notify-keyspace-events KEA
```

## Add Redis URL
metrics_adapter.py:

```python
import redis
import elasticsearch

#include Redis URL that ε-ORC is using
redis_client = redis.Redis(host='redisURL', port=6379, db=0)
```

## Run
```bash
git clone https://github.com/edgeless-project/redis-adapter.git
cd redis-adapter
python3 metrics_adapter.py
```
