import pandas as pd
import ast 
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import re

load_dotenv('db.env')
mongo_uri = os.getenv("ConnectionString")

print("MongoDB URI:", mongo_uri)

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client['CineMatchDB']
collection = db['Movies']

try:
    client.admin.command('ping')
    print("✅ Connected to MongoDB Atlas!")
except Exception as e:
    print("❌ Connection failed:", e)

df = pd.read_csv('MvpDataset.csv')

list_columns = ['Trope', 'StreamingServices', 'AllPeople', 'genres', 'titles']

def safe_list_parse(x, col_name):
    if pd.isna(x): return []
    
    # For 'AllPeople', handle it separately for better list parsing
    if col_name == 'AllPeople':
        # Split by spaces between the names and remove extra quotes and spaces
        parsed = re.findall(r"'([^']+)'", str(x))  # Extract everything between single quotes
        return [name.strip() for name in parsed if name.strip()]

    try:
        # If the value is already a list, leave it as is
        if isinstance(x, list):
            return x

        # Try parsing as a literal Python list
        parsed = ast.literal_eval(x)
        if isinstance(parsed, list): return parsed
        return [parsed]  # It's a single value like a string
    except (ValueError, SyntaxError):
        # Fallback: split on comma or space if no valid list is found
        return [item.strip() for item in str(x).split(',') if item.strip()]
    
for col in list_columns:
    df[col] = df[col].apply(lambda x: safe_list_parse(x, col))

df['isAdult'] = df['isAdult'].fillna(0).astype(int)
df['startYear'] = df['startYear'].fillna(0).astype(int)
df['runtimeMinutes'] = df['runtimeMinutes'].fillna(0).astype(int)
df['averageRating'] = df['averageRating'].fillna(0).astype(float)
df['numVotes'] = df['numVotes'].fillna(0).astype(int)

print("\nData Types Before Insertion:")
print(df[list_columns].applymap(type))

# Convert DataFrame to list of dictionaries
records = df.to_dict(orient='records')

collection.delete_many({})

# Insert into collection
collection.insert_many(records)

print("Inserted records successfully!")
