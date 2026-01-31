import pandas as pd
import sqlite3
import json

# 1. Load and Merge Data
orders = pd.read_csv('orders.csv')
with open('users.json', 'r') as f:
    users = pd.DataFrame(json.load(f))

conn = sqlite3.connect(':memory:')
with open('restaurants.sql', 'r') as f:
    conn.executescript(f.read())
restaurants = pd.read_sql_query("SELECT * FROM restaurants", conn)

df = pd.merge(orders, users, on='user_id', how='left')
df = pd.merge(df, restaurants, on='restaurant_id', how='left', suffixes=('', '_res'))

# --- Numerical Calculations ---

# 1. Total orders by Gold members
total_gold_orders = len(df[df['membership'] == 'Gold'])

# 2. Total revenue from Hyderabad (rounded to nearest integer)
hyd_revenue = round(df[df['city'] == 'Hyderabad']['total_amount'].sum())

# 3. Distinct users who placed at least one order
distinct_users = df['user_id'].nunique()

# 4. Average Order Value (AOV) for Gold members (rounded to 2 decimals)
gold_aov = round(df[df['membership'] == 'Gold']['total_amount'].mean(), 2)

# 5. Orders for restaurants with rating >= 4.5
rating_high_orders = len(df[df['rating'] >= 4.5])

# 6. Orders in the top revenue city among Gold members only
gold_df = df[df['membership'] == 'Gold']
top_gold_city = gold_df.groupby('city')['total_amount'].sum().idxmax()
top_city_gold_orders = len(gold_df[gold_df['city'] == top_gold_city])

print(f"Total Gold Orders: {total_gold_orders}")
print(f"Hyderabad Total Revenue: {hyd_revenue}")
print(f"Distinct Users: {distinct_users}")
print(f"Gold Member AOV: {gold_aov}")
print(f"Orders (Rating >= 4.5): {rating_high_orders}")
print(f"Orders in Top Gold City ({top_gold_city}): {top_city_gold_orders}")