from datetime import datetime, timedelta

# Function to check if a user has any orders
# data_orders: data of the second relation
# order_keys: all keys of the second relation
# user_id: key of entry from the first relation
# counter: dictionary with counters just for logging purposes
def check_user_has_orders(data_orders, order_keys, user_id, counter):    
    for order_key in order_keys:
        counter['semi-join_comparison_counter'] += 1
        order_data = data_orders.get(order_key)  # get all data from redis_db2 hashmap for given order key
        if order_data and order_data['user_id'] == user_id:  # check if order belongs to user
            return True  # user has at least one order
    return False  # no orders found for user


# Function to check and filter the orders
# data_orders: data of the second relation
# order_keys: all keys of the second relation
# user_id: key of entry from the first relation
# user_data: value for given key from the first relation
# counter: dictionary with counters just for logging purposes
def probe_and_filter_orders(data_orders, order_keys, user_id, user_data, counter):
    for order_key in list(order_keys):
        counter['hashjoin_comparison_counter'] += 1
        order_data = data_orders.get(order_key)  # get all data for given order key
        if order_data and order_data['user_id'] == user_id:  # check if order_data contain user_id
            user_reg_time = datetime.strptime(user_data['registration_timestamp'], '%Y-%m-%dT%H:%M:%S')
            order_time = datetime.strptime(order_data['order_timestamp'], '%Y-%m-%dT%H:%M:%S')
            if order_time <= user_reg_time + timedelta(weeks=1):  # filter joined tuples to include only orders placed within one week of user registration
                counter['hashjoin_counter'] += 1
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


# Semi-Join to ceck if user has any orders. If true Hash join to retrieve all orders of the user
def semi_join_filter_users(data_users, data_orders):
    counter = {'hashjoin_counter': 0, 'semi-join_counter': 0, 'hashjoin_comparison_counter': 0, 'semi-join_comparison_counter': 0}
    user_keys = list(data_users.keys())
    order_keys = list(data_orders.keys())
    print('### Results of combination of semi + hashjoin: ')
    for user_key in user_keys:
        user_data = data_users.get(user_key)  # get all data from redis_db1 hashmap for given user key
        if not check_user_has_orders(data_orders, order_keys, user_key, counter):
            counter['semi-join_counter'] += 1
            print({
                'user_id': user_key,
                'user_name': user_data['name'],
                'user_email': user_data['email'],
                'registration_timestamp': user_data['registration_timestamp']
            })
        else:
            probe_and_filter_orders(data_orders, order_keys, user_key, user_data, counter)

    return counter
