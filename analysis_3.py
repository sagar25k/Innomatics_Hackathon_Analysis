import pandas as pd

# 1. Verify row count
orders = pd.read_csv('orders.csv')
print(f"Total Rows: {len(orders)}")

# 2. Verify column origins
# Users file typically contains: user_id, name, city, membership
# Restaurants file contains: restaurant_id, restaurant_name, cuisine, rating

# 3. Verify join function
# The standard command is: pd.merge(left, right, on='key', how='left')