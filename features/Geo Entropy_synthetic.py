import pandas as pd
import numpy as np
import os
def main(files, output_folder):

    # Step 1: Extract all unique customer IDs across all files
    unique_customer_ids = set()

    for file in files.values():
        df = pd.read_csv(file)
        if "customer_id" in df.columns:
            unique_customer_ids.update(df["customer_id"].dropna().unique())

    # Convert to a DataFrame for consistency
    unique_customers_df = pd.DataFrame({"customer_id": list(unique_customer_ids)})

    # Step 2: Load and clean `card` and `abm` data
    card_df = pd.read_csv(files["card"])
    ach_df = pd.read_csv(files["ach"])
    wire_df = pd.read_csv(files["wire"])
    cheque_df = pd.read_csv(files["cheque"])

    # Keep relevant columns
    card_df = card_df[['customer_id', 'city','currency' ]]
    ach_df = ach_df[['customer_id', 'city', 'currency']]
    wire_df = wire_df[['customer_id', 'city', 'currency']]
    cheque_df = cheque_df[['customer_id', 'city', 'currency']]

    # Replace blank, NaN, or "other" with "unknown"
    for col in ['city', 'currency']:
        card_df[col] = card_df[col].replace(['other', np.nan], 'unknown')

    # Concatenate card and abm data
    combined_df = pd.concat([card_df, ach_df, wire_df, cheque_df], ignore_index=True)

    # Filter to include only transactions for the unique customers
    combined_df = combined_df[combined_df['customer_id'].isin(unique_customer_ids)]

    # Step 3: Create a single `location` column
    # combined_df['location'] = combined_df['country'] + '_' + combined_df['province'] + '_' + combined_df['city']

    # Step 4: Group transactions by customer_id and location and currency
    city_counts = combined_df.groupby(['customer_id', 'city']).size().reset_index(name='count')
    currency_counts = combined_df.groupby(['customer_id', 'currency']).size().reset_index(name='count')
    # Step 5: Calculate the total number of transactions for each customer
    city_total_transactions = city_counts.groupby('customer_id')['count'].sum().reset_index(name='total_count')
    currency_total_transactions = currency_counts.groupby('customer_id')['count'].sum().reset_index(name='total_count')

    # Merge the total transaction counts back into the location_counts DataFrame
    city_counts = city_counts.merge(city_total_transactions, on='customer_id')
    currency_counts = currency_counts.merge(currency_total_transactions, on='customer_id')

    # Step 6: Calculate the proportion of transactions for each location
    city_counts['proportion'] = city_counts['count'] / city_counts['total_count']
    currency_counts['proportion'] = currency_counts['count'] / currency_counts['total_count']

    # Step 7: Compute geographical entropy for each customer
    def calculate_entropy(group):
        proportions = group['proportion']
        entropy = -np.sum(proportions * np.log2(proportions))
        return entropy

    # Apply the entropy formula to each customer
    city_entropy = city_counts.groupby('customer_id').apply(calculate_entropy).reset_index(name='city_entropy')
    currency_entropy = currency_counts.groupby('customer_id').apply(calculate_entropy).reset_index(name='currency_entropy')
    # Step 8: Merge with the full list of unique customers
    # Mark customers not found in `card` or `abm` as `N/A`
    city_entropy = unique_customers_df.merge(
        city_entropy, on='customer_id', how='left'
    )
    currency_entropy = unique_customers_df.merge(
        currency_entropy, on='customer_id', how='left'
    )
    city_entropy['city_entropy'] = city_entropy['city_entropy'].fillna('N/A')
    currency_entropy['currency_entropy'] = currency_entropy['currency_entropy'].fillna('N/A')

    # Step 9: Save to CSV
    city_output_file = output_folder + "city_entropy_entropy.csv"
    city_entropy.to_csv(city_output_file, index=False)
    
    currency_output_file = output_folder + "currency_entropy.csv"
    currency_entropy.to_csv(currency_output_file, index=False)
    
    

    # Step 10: Print summary
    print(f"City entropy saved to {city_output_file}.")
    print(city_entropy.head())
    
    print(f"Currency entropy saved to {currency_output_file}.")
    print(currency_entropy.head())


if __name__ == "__main__":
    synthetic_data_dir = 'processed_synth_dataset/'
    files = os.listdir(synthetic_data_dir)
    file_dict = {}
    for file in files:
        file_dict[file.split('_')[0]] = synthetic_data_dir + file

    main(file_dict, synthetic_data_dir)
