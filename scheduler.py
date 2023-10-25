import redis
import os
import time
import requests
import json

class RedisResource:
    REDIS_QUEUE_LOCATION = os.getenv('REDIS_QUEUE', 'localhost')
    QUEUE_NAME = 'queue:encode'

    host, *port_info = REDIS_QUEUE_LOCATION.split(':')
    port = tuple()
    if port_info:
        port, *_ = port_info
        port = (int(port),)

    conn = redis.Redis(host=host, *port)

sub = RedisResource.conn.pubsub(ignore_subscribe_messages=True)
sub.subscribe("encode")

while True:
    msg = sub.get_message()
    if msg:
        print(f"new message in channel {msg['channel']}: {msg['data']}")
        url_post_chunk = "http://backend:80/chunk"
        url_post_thumbnail = "http://backend:80/thumbnail"
        requests.post(url_post_chunk, json=json.loads(msg['data']))
        requests.post(url_post_thumbnail, json=json.loads(msg['data']))
        time.sleep(10)
