import csv
import random
from datetime import datetime, timedelta

# Function to generate random orders for each user
def generate_orders(user_id):
    num_orders = random.randint(0, 10)  # Generate a random number of orders between 0 and 10
    orders = []
    start_date = datetime(2024, 6, 1, 12, 0, 0)  # Start date for random timestamp
    end_date = datetime(2024, 6, 23, 12, 0, 0)   # End date for random timestamp
    
    for i in range(num_orders):
        order_id = f"order_{user_id}_{i+1}" 
        product = f"product_{i+1}" 
        order_timestamp = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
        order_timestamp_str = order_timestamp.strftime('%Y-%m-%dT%H:%M:%S')
        orders.append([order_id, user_id, product, order_timestamp_str])
    return orders


# Generate users data
users_data = []
for i in range(100):
    user_id = f"user_{i+1}"
    name = f"User {i+1}"
    email = f"user{i+1}@example.com"
    registration_timestamp = f"2024-06-01T00:00:00"
    users_data.append([user_id, name, email, registration_timestamp])

# Write users data to CSV
users_csv_file = 'dataset/users100.csv'
with open(users_csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['user_id', 'name', 'email', 'registration_timestamp'])
    writer.writerows(users_data)

# Write orders data to CSV
orders_csv_file = 'dataset/orders100.csv'
with open(orders_csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['order_id', 'user_id', 'product', 'order_timestamp'])
    for user_data in users_data:
        user_id = user_data[0]  # user_id is the first element in the user_data list
        orders = generate_orders(user_id)
        writer.writerows(orders)

print(f"CSV files '{users_csv_file}' and '{orders_csv_file}' generated successfully.")
