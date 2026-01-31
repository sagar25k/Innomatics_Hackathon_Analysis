import pandas as pd
import sqlite3
import json

# 1. Load CSV (Transactional Data)
orders_df = pd.read_csv('orders.csv')

# 2. Load JSON (User Master Data)
with open('users.json', 'r') as f:
    users_df = pd.DataFrame(json.load(f))

# 3. Load SQL (Restaurant Master Data)
conn = sqlite3.connect(':memory:')
with open('restaurants.sql', 'r') as f:
    sql_script = f.read()
    conn.executescript(sql_script)
restaurants_df = pd.read_sql_query("SELECT * FROM restaurants", conn)

# 4. Merge Data (Left Join to retain all orders)
# Merge orders with user info
df = pd.merge(orders_df, users_df, on='user_id', how='left')
# Merge with restaurant info (suffixing names to avoid confusion)
df = pd.merge(df, restaurants_df, on='restaurant_id', how='left', suffixes=('_ord', '_res'))

# Clean date column for time analysis
df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True)

# ============================================================
# RESULTS GENERATION
# ============================================================

print("--- Final Dataset Analysis Results ---\n")

# Q1: Highest total revenue from Gold members
q1 = df[df['membership'] == 'Gold'].groupby('city')['total_amount'].sum().idxmax()
print(f"Q1: Highest Gold Revenue City: {q1}")

# Q2: Highest AOV by cuisine
q2 = df.groupby('cuisine')['total_amount'].mean().idxmax()
print(f"Q2: Highest AOV Cuisine: {q2}")

# Q3: Distinct users with total spend > 1000
user_spend = df.groupby('user_id')['total_amount'].sum()
q3 = (user_spend > 1000).sum()
print(f"Q3: Users with spend > 1000: {q3}")

# Q4: Rating range with highest total revenue
bins = [3.0, 3.5, 4.0, 4.5, 5.0]
labels = ['3.0 – 3.5', '3.6 – 4.0', '4.1 – 4.5', '4.6 – 5.0']
df['rating_range'] = pd.cut(df['rating'], bins=bins, labels=labels, include_lowest=True)
q4 = df.groupby('rating_range', observed=True)['total_amount'].sum().idxmax()
print(f"Q4: Highest Revenue Rating Range: {q4}")

# Q5: Gold members highest AOV by city
q5 = df[df['membership'] == 'Gold'].groupby('city')['total_amount'].mean().idxmax()
print(f"Q5: Highest Gold AOV City: {q5}")

# Q6: Cuisine with lowest distinct restaurants
q6 = df.groupby('cuisine')['restaurant_id'].nunique().idxmin()
print(f"Q6: Cuisine with lowest distinct restaurants: {q6}")

# Q7: Percentage of total orders by Gold members
q7 = round((len(df[df['membership'] == 'Gold']) / len(df)) * 100)
print(f"Q7: Percentage of Gold Orders: {q7}%")

# Q8: Highest AOV but < 20 orders
rest_stats = df.groupby('restaurant_name_ord').agg(aov=('total_amount', 'mean'), count=('order_id', 'count'))
q8 = rest_stats[rest_stats['count'] < 20]['aov'].idxmax()
print(f"Q8: Highest AOV with < 20 orders: {q8}")

# Q9: Highest revenue combination (Membership + Cuisine)
q9 = df.groupby(['membership', 'cuisine'])['total_amount'].sum().idxmax()
print(f"Q9: Highest Revenue Combo: {q9}")

# Q10: Peak Quarter
df['quarter'] = df['order_date'].dt.quarter
q10_map = {1: 'Q1 (Jan–Mar)', 2: 'Q2 (Apr-Jun)', 3: 'Q3 (Jul-Sep)', 4: 'Q4 (Oct-Dec)'}
q10 = q10_map[df.groupby('quarter')['total_amount'].sum().idxmax()]
print(f"Q10: Highest Revenue Quarter: {q10}")

# Save final dataset
df.to_csv('final_food_delivery_dataset.csv', index=False)