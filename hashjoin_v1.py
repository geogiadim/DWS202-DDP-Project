from datetime import datetime, timedelta

# Function to check and filter the orders
# data_orders: data of the second relation
# order_keys: all keys of the second relation
# user_id: key of entry from the first relation
# user_data: value for given key from the first relation
# counter: dictionary with counters just for logging purposes
def probe_and_filter_orders(data_orders, order_keys, user_id, user_data, counter):
    for order_key in list(order_keys):
        counter['comparison_counter'] += 1
        order_data = data_orders.get(order_key)  # get all data for given order key
        if order_data and order_data['user_id'] == user_id:  # check if order_data contain user_id
            user_reg_time = datetime.strptime(user_data['registration_timestamp'], '%Y-%m-%dT%H:%M:%S')
            order_time = datetime.strptime(order_data['order_timestamp'], '%Y-%m-%dT%H:%M:%S')
            if order_time <= user_reg_time + timedelta(weeks=1):  # filter joined tuples to include only orders placed within one week of user registration
                counter['join_counter'] += 1
                print({
                    'user_id': user_id,
                    'user_name': user_data['name'],
                    'user_email': user_data['email'],
                    'registration_timestamp': user_data['registration_timestamp'],
                    'order_id': order_key,
                    'product': order_data['product'],
                    'order_timestamp': order_data['order_timestamp']
                })
                order_keys.remove(order_key)  # Remove the key from the list since each order can be assigned to only one user


# Pipelined Hash Join
def pipelined_hash_join(data_users, data_orders, user_keys_param=[]):
    counter = {'join_counter': 0, 'comparison_counter': 0}

    # this is for getting the filtered user_keys from semi-join function
    if user_keys_param:
        user_keys = user_keys_param
    else:
        user_keys = list(data_users.keys())

    order_keys = list(data_orders.keys()) 
    print('### Results of hashjoin v1: ')
    for user_key in user_keys:
        user_data = data_users.get(user_key)
        probe_and_filter_orders(data_orders, order_keys, user_key, user_data, counter)
    
    return counter 


# # Function to check and filter the orders
# # redis_conn2: db of the second relation
# # user_id: key of entry from the first relation
# # user_data: value for given key from the first relation
# def probe_and_filter_orders(redis_conn2, user_id, user_data, comparison_counter, join_counter):
#     order_keys = redis_conn2.keys()  # get all order keys from redis_db2

#     for order_key in order_keys:
#         comparison_counter['count'] += 1
#         order_data = redis_conn2.hgetall(order_key)  # get all data from redis_db2 for given order key
#         if order_data and order_data["b'user_id'"].decode('utf-8') == user_id.decode('utf-8'):  # check if order_data contain user_id
#             user_reg_time = datetime.strptime(user_data["b'registration_timestamp'"], '%Y-%m-%dT%H:%M:%S')
#             order_time = datetime.strptime(order_data["b'order_timestamp'"], '%Y-%m-%dT%H:%M:%S')
#             if order_time <= user_reg_time + timedelta(weeks=1):  # filter joined tuples to include only orders placed within one week of user registration
#                 join_counter['count'] += 1
#                 print({
#                     'user_id': user_id,
#                     'user_name': user_data["b'name'"],
#                     'user_email': user_data["b'email'"],
#                     'registration_timestamp': user_data["b'registration_timestamp'"],
#                     'order_id': order_key,
#                     'product': order_data["b'product'"],
#                     'order_timestamp': order_data["b'order_timestamp'"]
#                 })


# # Pipelined Hash Join
# def pipelined_hash_join(redis_conn1, redis_conn2, user_keys_param=[]):
#     comparison_counter = {'count': 0}
#     join_counter = {'count': 0}
#     if user_keys_param:
#         user_keys = user_keys_param.encode()
#     else:
#         user_keys = redis_conn1.keys()  # get all user keys from redis_db1
        
#     for user_key in user_keys:
#         user_data = redis_conn1.hgetall(user_key)  # get all data from redis_db1 for given user key  
#         if user_data:
#             probe_and_filter_orders(redis_conn2, user_key.decode('utf-8'), user_data, comparison_counter, join_counter)
#     print(f"Total joins performed in hashjoin v1: {join_counter['count']}")
#     print(f"Total comparisons in Version 1: {comparison_counter['count']}")


# # Function to perform pipelined hash join and filter orders placed within a week of user registration
# def pipelined_hash_join(redis_conn1, redis_conn2):
#     counter = {'join_counter': 0, 'comparison_counter': 0}
#     # Iterate over user keys in redis_conn1
#     for user_key in redis_conn1.scan_iter("*"):
#         user_data = redis_conn1.hgetall(user_key)
#         user_id = user_key.decode('utf-8')

#         # Start pipeline for fetching orders related to the current user_id
#         pipeline = redis_conn2.pipeline()

#         # Fetch orders for current user_id
#         for order_key in redis_conn2.scan_iter("*"):
#             order_data = redis_conn2.hgetall(order_key)
#             if order_data and order_data[b'user_id'].decode('utf-8') == user_id:
#                 counter['comparison_counter'] += 1
#                 order_timestamp = datetime.strptime(order_data[b'order_timestamp'].decode('utf-8'), '%Y-%m-%dT%H:%M:%S')
#                 user_reg_time = datetime.strptime(user_data[b'registration_timestamp'].decode('utf-8'), '%Y-%m-%dT%H:%M:%S')
#                 if order_timestamp <= user_reg_time + timedelta(weeks=1):
#                     counter['join_counter'] += 1
#                     # Print or process the filtered orders
#                     print({
#                         'user_id': user_id,
#                         'user_name': user_data[b'name'].decode('utf-8'),
#                         'user_email': user_data[b'email'].decode('utf-8'),
#                         'registration_timestamp': user_data[b'registration_timestamp'].decode('utf-8'),
#                         'order_id': order_key.decode('utf-8'),
#                         'product': order_data[b'product'].decode('utf-8'),
#                         'order_timestamp': order_data[b'order_timestamp'].decode('utf-8')
#                     })

#         # Execute pipeline (batch retrieval of orders)
#         pipeline.execute()

#     return counter
