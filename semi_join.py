from datetime import datetime

# Function to check if a user has any orders
def check_user_has_orders(redis_conn2, user_id, comparison_counter):
    order_keys = redis_conn2.keys()  # get all order keys from redis_db2
    
    for order_key in order_keys:
        comparison_counter['count'] += 1
        order_data = redis_conn2.hgetall(order_key)  # get all data from redis_db2 for given order key
        order_data_decoded = {k.decode('utf-8'): v.decode('utf-8') for k, v in order_data.items()}
        if order_data_decoded and order_data_decoded['user_id'] == user_id:  # check if order belongs to user
            return True  # User has at least one order
    return False  # No orders found for user


# Semi-Join to find users without any orders
def semi_join_users_without_orders(redis_conn1, redis_conn2):
    comparison_counter = {'count': 0}
    user_keys = redis_conn1.keys()  # get all user keys from redis_db1

    for user_key in user_keys:
        user_data = redis_conn1.hgetall(user_key)  # get all data from redis_db1 for given user key
        if user_data:
            user_data_decoded = {k.decode('utf-8'): v.decode('utf-8') for k, v in user_data.items()}
            user_id = user_key.decode('utf-8')
            if not check_user_has_orders(redis_conn2, user_id, comparison_counter):
                print({
                    'user_id': user_id,
                    'user_name': user_data_decoded['name'],
                    'user_email': user_data_decoded['email'],
                    'registration_timestamp': user_data_decoded['registration_timestamp']
                })
    
    print(f"Total comparisons in semi-join: {comparison_counter['count']}")