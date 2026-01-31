import pandas as pd
import sqlite3
import json

# 1. Load CSV Data
orders_df = pd.read_csv('orders.csv')

# 2. Load JSON Data
users_df = pd.read_json('users.json')

# 3. Load SQL Data (Restaurants)
# Create an in-memory database to execute the .sql file
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

with open('restaurants.sql', 'r') as f:
    sql_script = f.read()
    cursor.executescript(sql_script)

# Fetch data from the created table into a DataFrame
restaurants_df = pd.read_sql_query("SELECT * FROM restaurants", conn)

# 4. Merge the Data (Left Joins)
# Join orders with users first
merged_df = pd.merge(orders_df, users_df, on='user_id', how='left')

# Join the result with restaurants
# Note: suffixes=['_orders', '_rest'] helps if there are overlapping column names
final_df = pd.merge(merged_df, restaurants_df, on='restaurant_id', how='left')

# 5. Export to the required filename
final_df.to_csv('final_food_delivery_dataset.csv', index=False)

print("Success! 'final_food_delivery_dataset.csv' has been created.")