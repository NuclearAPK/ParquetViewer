import pandas as pd

# Create sample data using a simple DataFrame constructor with proper column names
df = pd.DataFrame({
    'id': list(range(1, 51)),  # 50 elements (1 to 50)
    'name': [f"User_{i}" for i in range(1, 51)],  # 50 elements
    'age': [25 + (i % 30) for i in range(50)],  # 50 elements
    'score': [round(50 + i * 2.5, 1) for i in range(50)]  # 50 elements
})

# Add department column separately
departments = ['Engineering', 'Marketing', 'Sales', 'HR']
df['department'] = [departments[i % 4] for i in range(50)]

# Save as Parquet using pandas
df.to_parquet("sample_data.parquet")

print(f"Created Parquet file with {len(df)} rows and {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")
print(f"First 3 rows:")
print(df.head(3))