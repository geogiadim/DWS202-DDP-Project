from datetime import datetime, timedelta

# Function to check and filter the users
# redis_conn1: db of the first relation
# order_key: key of entry from the second relation
# order_data: value for given key from the second relation
def probe_and_filter_orders(redis_conn1, order_key, order_data, comparison_counter):
    user_keys = redis_conn1.keys()  # get all user keys from redis_db1

    for user_key in user_keys:
        comparison_counter['count'] += 1
        user_key_decoded = user_key.decode('utf-8')
        user_data = redis_conn1.hgetall(user_key)  # get all data from redis_db1 for given user key
        user_data_decoded = {k.decode('utf-8'): v.decode('utf-8') for k, v in user_data.items()}
        if user_data_decoded and user_key_decoded == order_data['user_id']:  # check if user_id is referred to order_data
            user_reg_time = datetime.strptime(user_data_decoded['registration_timestamp'], '%Y-%m-%dT%H:%M:%S')
            order_time = datetime.strptime(order_data['order_timestamp'], '%Y-%m-%dT%H:%M:%S')
            if order_time <= user_reg_time + timedelta(weeks=1):  # filter joined tuples to include only orders placed within one week of user registration
                print({
                    'user_id': user_key.decode('utf-8'),
                    'user_name': user_data_decoded['name'],
                    'user_email': user_data_decoded['email'],
                    'registration_timestamp': user_data_decoded['registration_timestamp'],
                    'order_id': order_key,
                    'product': order_data['product'],
                    'order_timestamp': order_data['order_timestamp']
                })
       

# Pipelined Hash Join
def pipelined_hash_join(redis_conn1, redis_conn2):
    comparison_counter = {'count': 0}

    order_keys = redis_conn2.keys()  # get all order keys from redis_db2  

    for order_key in order_keys:
        order_data = redis_conn2.hgetall(order_key)  # get all data from redis_db2 for given order key  
        if order_data:
            order_data_decoded = {k.decode('utf-8'): v.decode('utf-8') for k, v in order_data.items()}
            probe_and_filter_orders(redis_conn1, order_key.decode('utf-8'), order_data_decoded, comparison_counter)
    
    print(f"Total comparisons in Version 2: {comparison_counter['count']}")
