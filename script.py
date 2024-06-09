import os
from dotenv import load_dotenv
import redis
from pyignite import Client
import random
import datetime

# Load environment variables from .env file
load_dotenv()

# Setup connection to Redis db
def setup_redis_connection(host, port):
    redis_client = redis.Redis(host=host, port=port)
    return redis_client

# Setup connection to Ignite db
def setup_ignite_connection(host, port):
    client = Client()
    client.connect(str(host), int(port))
    return client

# Function to populate Redis with random data
def populate_redis(r, num_entries=100):
    for i in range(num_entries):
        value = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))
        timestamp = datetime.datetime.now().isoformat()
        r.hmset(f'table1:{i}', {'value': value, 'timestamp': timestamp})
    
    for i in range(num_entries):
        value = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))
        table1_id = random.randint(0, num_entries - 1)
        timestamp = datetime.datetime.now().isoformat()
        r.hmset(f'table2:{i}', {'table1_id': table1_id, 'value': value, 'timestamp': timestamp})

# Function to populate Ignite with random data
def populate_ignite(client, num_entries=100):
    client.sql('CREATE TABLE IF NOT EXISTS table1 (id INT PRIMARY KEY, value VARCHAR, timestamp TIMESTAMP)')
    client.sql('CREATE TABLE IF NOT EXISTS table2 (id INT PRIMARY KEY, table1_id INT, value VARCHAR, timestamp TIMESTAMP)')

    for i in range(num_entries):
        value = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))
        timestamp = datetime.datetime.now()
        client.sql('INSERT INTO table1 (id, value, timestamp) VALUES (?, ?, ?)', query_args=[i, value, timestamp])
    
    for i in range(num_entries):
        value = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))
        table1_id = random.randint(0, num_entries - 1)
        timestamp = datetime.datetime.now()
        client.sql('INSERT INTO table2 (id, table1_id, value, timestamp) VALUES (?, ?, ?, ?)', query_args=[i, table1_id, value, timestamp])

def main():
    redis_conn = setup_redis_connection(os.getenv('REDIS_HOST'), os.getenv('REDIS_PORT'))
    ignite_conn = setup_ignite_connection(os.getenv('IGNITE_CLIENT_HOST'), os.getenv('IGNITE_CLIENT_PORT'))

    # # Populate Redis
    # populate_redis(redis_conn)
    
    # # Populate Ignite
    # populate_ignite(ignite_conn)

    # Perform joins and other operations as needed
    # (To be implemented according to your requirements)

if __name__ == '__main__':
    main()
