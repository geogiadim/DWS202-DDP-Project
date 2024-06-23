import os
import time
from dotenv import load_dotenv
import redis
import pandas as pd
import hashjoin_v1, hashjoin_v2, semi_join

load_dotenv()

# Populate redis_db1 instance with df containing users entries  
def populate_user_data(redis_conn, df):
    for index, row in df.iterrows():
        timestamp_str = str(row['registration_timestamp'])
        redis_conn.hset(row['user_id'], mapping={
                                            'name': row['name'],
                                            'email': row['email'],
                                            'registration_timestamp': timestamp_str
                                        })


# Populate redis_db2 instance with df containing orders entries
def populate_order_data(redis_conn, df):
    for index, row in df.iterrows():
        timestamp_str = str(row['order_timestamp'])
        redis_conn.hset(row['order_id'], mapping={
                                            'user_id': row['user_id'],
                                            'product': row['product'],
                                            'order_timestamp': timestamp_str
                                        }) 


def main():
    # setup connections with 2 redis db
    redis_conn1 = redis.Redis(os.getenv('REDIS_HOST1'), os.getenv('REDIS_DEFAULT_INTERNAL_PORT'))
    redis_conn2 = redis.Redis(os.getenv('REDIS_HOST2'), os.getenv('REDIS_DEFAULT_INTERNAL_PORT'))

    # read the csv records (tables) in df 
    df_users = pd.read_csv(os.getenv('USERS_IN_DOCKER_PATH'))
    df_orders = pd.read_csv(os.getenv('ORDERS_IN_DOCKER_PATH'))
    
    # populate both redis instances with corresponding data
    populate_user_data(redis_conn1, df_users)
    populate_order_data(redis_conn2, df_orders)

    # Perform the v1 pipelined hash join (hash the small relation)
    start_time = time.time()
    hashjoin_v1.pipelined_hash_join(redis_conn1, redis_conn2)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"The execution time of hash join v1 is {execution_time} seconds")

    # Perform the v2 pipelined hash join (hash the big relation)
    start_time = time.time()
    hashjoin_v2.pipelined_hash_join(redis_conn1, redis_conn2)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"The execution time hash join v2 is {execution_time} seconds")

    # Perform semi-join to retrieve users without any order
    start_time = time.time()
    semi_join.semi_join_users_without_orders(redis_conn1, redis_conn2)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"The execution time semi-join is {execution_time} seconds")


if __name__ == '__main__':
    main()
