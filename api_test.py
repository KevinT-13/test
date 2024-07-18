import pandas as pd
from sodapy import Socrata
from requests.exceptions import ReadTimeout, ConnectTimeout
import time

# Define your Socrata credentials (app token is optional but recommended for higher rate limits)
app_token = None # Replace with your app token or leave as an empty string
client = Socrata("data.cityofnewyork.us", app_token, timeout=120)  # Set a 120-second timeout

# Define the dataset identifier and query parameters
dataset_identifier = "wvxf-dwi5"
where_clause = "ViolationStatus='Close' AND (class='B' OR class='c') AND novissueddate < '2024-07-19T00:00:00'"
batch_size = 50  # Fetch data in smaller batches
max_records = 100  # Total number of records to fetch
offset = 0

# Function to fetch data with retry mechanism
def fetch_data_with_retry(client, dataset_identifier, where_clause, limit, offset, retries=3):
    for attempt in range(retries):
        try:
            # Fetch the data using the Socrata client
            results = client.get(dataset_identifier, where=where_clause, limit=limit, offset=offset)
            return results
        except (ReadTimeout, ConnectTimeout) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print("Retrying...")
                time.sleep(5)  # Wait for 5 seconds before retrying
            else:
                print("Max retries reached. Exiting.")
                raise

# Initialize an empty list to store the results
all_results = []

# Fetch data in smaller batches
while offset < max_records:
    results = fetch_data_with_retry(client, dataset_identifier, where_clause, batch_size, offset)
    all_results.extend(results)
    offset += batch_size
    if len(results) < batch_size:  # Stop if fewer records are returned than requested
        break

# Convert the results to a pandas DataFrame
df = pd.DataFrame.from_records(all_results)

# Display the DataFrame
print(df)
