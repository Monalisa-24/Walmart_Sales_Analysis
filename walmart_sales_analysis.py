import pandas as pd
import mysql.connector
from datetime import datetime



# Load dataset
df = pd.read_csv("Walmart.csv")

# Step 2: Clean the dataset
# Remove dollar signs and convert unit_price to float
df['unit_price'] = df['unit_price'].replace('[\\$,]', '', regex=True).astype(float)

# Convert date to datetime
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%y')

# Convert time to proper time format
df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.time

# Handle quantity: drop rows with missing values and convert to int
df = df.dropna(subset=['quantity'])
df['quantity'] = df['quantity'].astype(int)

# Handle rating and profit_margin similarly
df = df.dropna(subset=['rating', 'profit_margin'])
df['rating'] = df['rating'].astype(float)
df['profit_margin'] = df['profit_margin'].astype(float)

# Step 3: Generate MySQL CREATE TABLE statement
# Connect to MySQL
# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Monalisa@2001",
    database="walmart_db"
)

if conn.is_connected():
    print("Connected to MySQL!")

cursor = conn.cursor()

try:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS walmart_sales (
        invoice_id VARCHAR(20),
        Branch VARCHAR(10),
        City VARCHAR(50),
        category VARCHAR(50),
        unit_price FLOAT,
        quantity INT,
        date DATE,
        time TIME,
        payment_method VARCHAR(20),
        rating FLOAT,
        profit_margin FLOAT
    );
    """)
    print("Table created or already exists.")
except mysql.connector.Error as err:
    print(f"Error during table creation: {err}")

# Confirm table exists
cursor.execute("SHOW TABLES LIKE 'walmart_sales';")
if cursor.fetchone():
    print("✅ Table 'walmart_sales' exists.")
else:
    print("❌ Table was not created.")


# Show one sample cleaned row
sample_row = df.iloc[0].to_dict()
print("Sample cleaned row:")
for key, value in sample_row.items():
    print(f"{key}: {value}")


# Insert data row-by-row
for _, row in df.iterrows():
    try:
        cursor.execute("""
            INSERT INTO walmart_sales (
                invoice_id, Branch, City, category, unit_price, quantity,
                date, time, payment_method, rating, profit_margin
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['invoice_id'], row['Branch'], row['City'], row['category'],
            row['unit_price'], int(row['quantity']),
            row['date'].date(),  # convert pandas Timestamp to date
            row['time'].strftime('%H:%M:%S') if pd.notnull(row['time']) else None,  # convert time to string
            row['payment_method'], float(row['rating']), float(row['profit_margin'])
        ))
    except Exception as e:
        print("Insert error:", e)
conn.commit()
