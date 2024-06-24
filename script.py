import os
import time
from dotenv import load_dotenv
import redis
import pandas as pd
import hashjoin_v1, hashjoin_v2, semi_join, combo_semi_hash_v1

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


def fetch_all_data(redis_conn):
    all_data = {}
    for key in redis_conn.scan_iter("*"):  # Fetch all keys
        data = redis_conn.hgetall(key)  # Assuming data is stored in hashes
        all_data[key.decode('utf-8')] = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
    return all_data


def main():
    # setup connections with 2 redis db
    redis_conn1 = redis.Redis(os.getenv('REDIS_HOST1'), os.getenv('REDIS_DEFAULT_INTERNAL_PORT'))
    redis_conn2 = redis.Redis(os.getenv('REDIS_HOST2'), os.getenv('REDIS_DEFAULT_INTERNAL_PORT'))

    # # read the csv records (tables) in df 
    # df_users = pd.read_csv(os.getenv('USERS_IN_DOCKER_PATH'))
    # df_orders = pd.read_csv(os.getenv('ORDERS_IN_DOCKER_PATH'))
    
    # # populate both redis instances with corresponding data
    # populate_user_data(redis_conn1, df_users)
    # populate_order_data(redis_conn2, df_orders)

    # fetch all data from each redis db so as to avoid multiple calls in redis which is time-consuming process
    all_data_users = fetch_all_data(redis_conn1)
    all_data_orders = fetch_all_data(redis_conn2)

    # Perform the v1 pipelined hash join (hash the small relation)
    start_time = time.time()
    counter_v1 = hashjoin_v1.pipelined_hash_join(all_data_users, all_data_orders)
    end_time = time.time()
    execution_time_v1 = end_time - start_time

    # Perform the v2 pipelined hash join (hash the big relation)
    start_time = time.time()
    counter_v2 = hashjoin_v2.pipelined_hash_join(all_data_users, all_data_orders)
    end_time = time.time()
    execution_time_v2 = end_time - start_time

    # Perform semi-join to retrieve users without any order
    start_time = time.time()
    counter_semi = semi_join.semi_join_users_without_orders(all_data_users, all_data_orders)
    end_time = time.time()
    execution_time_semi = end_time - start_time

    # Perform semi-join to check if uses has any order. if True then perform hashjoin_v1 to get all the orders of the user
    start_time = time.time()
    counter_combo = combo_semi_hash_v1.semi_join_filter_users(all_data_users, all_data_orders)
    end_time = time.time()
    execution_time_combo = end_time - start_time
    
    # results
    print('####################################')
    print(f"The execution time of hash join v1 is {execution_time_v1} seconds")
    print(f"Total comparisons performed in hash join v1: {counter_v1['comparison_counter']}")
    print(f"Total hash joins v1: {counter_v1['join_counter']}")
    print('####################################')
    print(f"The execution time of hash join v2 is {execution_time_v2} seconds")
    print(f"Total comparisons performed in hash join v2: {counter_v2['comparison_counter']}")
    print(f"Total hash joins v2: {counter_v2['join_counter']}")
    print('####################################')
    print(f"The execution time semi-join is {execution_time_semi} seconds")
    print(f"Total comparisons performed in semi join: {counter_semi['comparison_counter']}")
    print(f"Total semi joins: {counter_semi['join_counter']}")
    print('####################################')
    print(f"The execution time of combo is {execution_time_combo} seconds")
    print(f"Total comparisons performed in combo (semi): {counter_combo['semi-join_comparison_counter']}")
    print(f"Total comparisons performed in combo (hash): {counter_combo['hashjoin_comparison_counter']}")
    print(f"Total comparisons performed in combo (semi + hash): {counter_combo['hashjoin_comparison_counter'] + counter_combo['semi-join_comparison_counter']}")
    print(f"Total hash joins in combo: {counter_combo['hashjoin_counter']}")
    print(f"Total semi joins in combo: {counter_combo['semi-join_counter']}")
    print('####################################')


if __name__ == '__main__':
    main()
