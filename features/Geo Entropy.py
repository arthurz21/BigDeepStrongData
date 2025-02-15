import pandas as pd
import numpy as np

# File paths
files = {
    "card": "card.csv",  # Update with your file paths
    "abm": "abm.csv",
    "cheque": "cheque.csv",
    "eft": "eft.csv",
    "emt": "emt.csv",
    # "kyc": "kyc.csv",
    # "kyc_industry_codes": "kyc_industry_codes.csv",
    "wire": "wire.csv"
}

# Step 1: Extract all unique customer IDs across all files
unique_customer_ids = set()

for file in files.values():
    df = pd.read_csv('raw_data/' + file)
    if "customer_id" in df.columns:
        unique_customer_ids.update(df["customer_id"].dropna().unique())

# Convert to a DataFrame for consistency
unique_customers_df = pd.DataFrame({"customer_id": list(unique_customer_ids)})

# Step 2: Load and clean `card` and `abm` data
card_df = pd.read_csv('raw_data/' + files["card"])
abm_df = pd.read_csv('raw_data/' + files["abm"])

# Keep relevant columns
card_df = card_df[['customer_id', 'country', 'province', 'city']]
abm_df = abm_df[['customer_id', 'country', 'province', 'city']]

# Replace blank, NaN, or "other" with "unknown"
for col in ['country', 'province', 'city']:
    card_df[col] = card_df[col].replace(['other', np.nan], 'unknown')
    abm_df[col] = abm_df[col].replace(['other', np.nan], 'unknown')

# Concatenate card and abm data
combined_df = pd.concat([card_df, abm_df], ignore_index=True)

# Filter to include only transactions for the unique customers
combined_df = combined_df[combined_df['customer_id'].isin(unique_customer_ids)]

# Step 3: Create a single `location` column
combined_df['location'] = combined_df['country'] + '_' + combined_df['province'] + '_' + combined_df['city']

# Step 4: Group transactions by customer_id and location
location_counts = combined_df.groupby(['customer_id', 'location']).size().reset_index(name='count')

# Step 5: Calculate the total number of transactions for each customer
total_transactions = location_counts.groupby('customer_id')['count'].sum().reset_index(name='total_count')

# Merge the total transaction counts back into the location_counts DataFrame
location_counts = location_counts.merge(total_transactions, on='customer_id')

# Step 6: Calculate the proportion of transactions for each location
location_counts['proportion'] = location_counts['count'] / location_counts['total_count']

# Step 7: Compute geographical entropy for each customer
def calculate_entropy(group):
    proportions = group['proportion']
    entropy = -np.sum(proportions * np.log2(proportions))
    return entropy

# Apply the entropy formula to each customer
geographical_entropy = location_counts.groupby('customer_id').apply(calculate_entropy).reset_index(name='geographical_entropy')

# Step 8: Merge with the full list of unique customers
# Mark customers not found in `card` or `abm` as `N/A`
geographical_entropy = unique_customers_df.merge(
    geographical_entropy, on='customer_id', how='left'
)
geographical_entropy['geographical_entropy'] = geographical_entropy['geographical_entropy'].fillna(0)

# Step 9: Save to CSV
output_file = "features/geographical_entropy.csv"
geographical_entropy.to_csv(output_file, index=False)

# Step 10: Print summary
print(f"Geographical entropy saved to {output_file}.")
print(geographical_entropy.head())
