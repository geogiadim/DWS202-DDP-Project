# Function to check if a user has any orders
# data_orders: data of the second relation
# user_id: key of entry from the first relation
# counter: dictionary with counters just for logging purposes
def check_user_has_orders(data_orders, user_id, counter):
    order_keys = []
    for order_key, order_data in data_orders.items():
        order_keys.append(order_key)
    
    for order_key in order_keys:
        counter['comparison_counter'] += 1
        order_data = data_orders.get(order_key)  # get all data from redis_db2 hashmap for given order key
        if order_data and order_data['user_id'] == user_id:  # check if order belongs to user
            return True  # user has at least one order
    return False  # no orders found for user


# Semi-Join to find users without any orders
def semi_join_users_without_orders(data_users, data_orders):
    counter = {'join_counter': 0, 'comparison_counter': 0}
    user_keys = []
    for user_key, user_data in data_users.items():
        user_keys.append(user_key)

    users_with_orders = []
    for user_key in user_keys:
        user_data = data_users.get(user_key)  # get all data from redis_db1 hashmap for given user key
        if user_data:
            if not check_user_has_orders(data_orders, user_key, counter):
                counter['join_counter'] += 1
                print({
                    'user_id': user_key,
                    'user_name': user_data['name'],
                    'user_email': user_data['email'],
                    'registration_timestamp': user_data['registration_timestamp']
                })
            else:
                users_with_orders.append(user_key)
    
    return counter, users_with_orders