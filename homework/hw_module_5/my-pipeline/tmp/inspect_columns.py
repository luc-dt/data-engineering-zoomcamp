import pandas as pd
import requests
import io

# Download just ONE file to explore
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
response = requests.get(url)
df = pd.read_parquet(io.BytesIO(response.content))

# See all column names
print(df.columns.tolist())

# See data types
print(df.dtypes)

# See shape
print(df.shape)
