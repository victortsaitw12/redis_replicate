# redis_replicate

1. node exec.js old_connection new_connection [Redis Instances]
2. Scan the keys from Redis Instances
3. Save Keys into the set to filter the duplicate keys.
4. Fetch the data from old Redis Proxy.
5. Save the data into the new Redis Proxy.
6. Print the total getting keys and inserttng key to console.
