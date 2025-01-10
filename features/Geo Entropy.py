import pandas as pd
import numpy as np

# Load the datasets
card_file = "card.csv"  # Update with your file path
abm_file = "abm.csv"  # Update with your file path

card_df = pd.read_csv(card_file)
abm_df = pd.read_csv(abm_file)

# Step 1: Standardize column names for consistency
# Ensure both datasets have the same column names
card_df = card_df[['customer_id', 'country', 'province', 'city']]  # Keep only relevant columns
abm_df = abm_df[['customer_id', 'country', 'province', 'city']]  # Adjust for ABM column names

# Rename columns in abm_df to match card_df
abm_df.rename(columns={'customer': 'customer_id'}, inplace=True)

# Step 2: Replace blank, NaN, or "other" with "unknown"
for col in ['country', 'province', 'city']:
    card_df[col] = card_df[col].replace(['other', np.nan], 'unknown')
    abm_df[col] = abm_df[col].replace(['other', np.nan], 'unknown')

# Step 3: Concatenate both datasets
combined_df = pd.concat([card_df, abm_df], ignore_index=True)

# Step 4: Create a single `location` column by combining `country`, `province`, and `city`
combined_df['location'] = combined_df['country'] + '_' + combined_df['province'] + '_' + combined_df['city']

# Step 5: Group transactions by customer_id and location
location_counts = combined_df.groupby(['customer_id', 'location']).size().reset_index(name='count')

# Step 6: Calculate the total number of transactions for each customer
total_transactions = location_counts.groupby('customer_id')['count'].sum().reset_index(name='total_count')

# Merge the total transaction counts back into the location_counts DataFrame
location_counts = location_counts.merge(total_transactions, on='customer_id')

# Step 7: Calculate the proportion of transactions for each location
location_counts['proportion'] = location_counts['count'] / location_counts['total_count']

# Step 8: Compute geographical entropy for each customer
def calculate_entropy(group):
    proportions = group['proportion']
    entropy = -np.sum(proportions * np.log2(proportions))
    return entropy

# Apply the entropy formula to each customer
geographical_entropy = location_counts.groupby('customer_id').apply(calculate_entropy).reset_index(name='geographical_entropy')

# Step 9: Save or display the results
print(geographical_entropy)

# Save to CSV if needed
geographical_entropy.to_csv("geographical_entropy.csv", index=False)
