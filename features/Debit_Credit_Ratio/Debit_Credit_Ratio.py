import pandas as pd
import os
from pathlib import Path
import numpy as np


def standardize_credit_debit(value):
    """
    Standardize credit/debit indicators to 'credit' or 'debit'
    """
    if pd.isna(value):
        return None
    value = str(value).strip().lower()
    if value in ['c', 'credit']:
        return 'credit'
    elif value in ['d', 'debit']:
        return 'debit'
    return None


def process_transaction_files(folder_path):
    """
    Process transaction CSV files to create customer-level summary of credit and debit amounts.
    """
    final_df = None
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if 'kyc.csv' in csv_files: 
        csv_files.remove('kyc.csv')
    if 'kyc_industry_codes.csv' in csv_files: 
        csv_files.remove('kyc_industry_codes.csv')

    for file in csv_files:
        try:
            # Extract channel name from filename
            channel = file.split('.')[0].lower()
            print(f"\nProcessing {channel} transactions...")

            # Read the CSV file
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            
            # Because all the credited transactions are negative in the card.csv file 
            # But all the other type of transactions are positive for both credited and debited transactions. 
            if file == 'card.csv':
                df['amount_cad'] = df['amount_cad'].abs()

            # Standardize debit_credit column values
            df['debit_credit'] = df['debit_credit'].apply(standardize_credit_debit)

            # Calculate credit and debit amounts
            credit_df = df[df['debit_credit'] == 'credit'].groupby('customer_id').agg({
                'amount_cad': lambda x: np.sum(np.abs(x))
            }).rename(columns={'amount_cad': f'{channel}_credit_amount'})

            debit_df = df[df['debit_credit'] == 'debit'].groupby('customer_id').agg({
                'amount_cad': lambda x: np.sum(np.abs(x))
            }).rename(columns={'amount_cad': f'{channel}_debit_amount'})

            # Merge credit and debit summaries
            channel_summary = pd.merge(credit_df, debit_df,
                                       how='outer',
                                       left_index=True,
                                       right_index=True)

            # Reset index to make customer_id a column
            channel_summary.reset_index(inplace=True)

            # Add count columns
            credit_counts = df[df['debit_credit'] == 'credit']['customer_id'].value_counts()
            debit_counts = df[df['debit_credit'] == 'debit']['customer_id'].value_counts()

            channel_summary[f'{channel}_credit_count'] = channel_summary['customer_id'].map(credit_counts).fillna(0)
            channel_summary[f'{channel}_debit_count'] = channel_summary['customer_id'].map(debit_counts).fillna(0)

            # Fill NaN values with 0
            channel_summary = channel_summary.fillna(0)

            # Merge with final_df
            if final_df is None:
                final_df = channel_summary
            else:
                final_df = pd.merge(final_df, channel_summary,
                                    on='customer_id',
                                    how='outer')

            print(f"Processed {channel}:")
            print(f"- Total credit transactions: {int(channel_summary[f'{channel}_credit_count'].sum())}")
            print(f"- Total credit amount: ${channel_summary[f'{channel}_credit_amount'].sum():,.2f}")
            print(f"- Total debit transactions: {int(channel_summary[f'{channel}_debit_count'].sum())}")
            print(f"- Total debit amount: ${channel_summary[f'{channel}_debit_amount'].sum():,.2f}")

        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
            if 'df' in locals():
                print(f"Columns in the file: {df.columns.tolist()}")
            if final_df is not None:
                print(f"Columns in final_df: {final_df.columns.tolist()}")

    # Fill any remaining NaN values with 0
    if final_df is not None:
        final_df = final_df.fillna(0)

        # Sort columns alphabetically after customer_id
        cols = ['customer_id'] + sorted([col for col in final_df.columns if col != 'customer_id'])
        final_df = final_df[cols]

        # Sort by customer_id
        final_df = final_df.sort_values('customer_id')

    return final_df


def save_results(df, folder_path, filename="transaction_credit_debit_summary.csv"):
    """
    Save results and print summary statistics
    """
    if df is not None:
        # Save to CSV
        
        # Create output directory if it doesn't exist
        Path(folder_path).mkdir(parents=True, exist_ok=True)
        
        output_file = os.path.join(folder_path, filename)
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")

        # Print overall summary
        print("\nOverall Summary:")
        channels = set(col.split('_')[0] for col in df.columns
                       if col != 'customer_id' and any(x in col for x in ['credit', 'debit']))

        for channel in sorted(channels):
            credit_amount = df[f'{channel}_credit_amount'].sum()
            debit_amount = df[f'{channel}_debit_amount'].sum()
            credit_count = df[f'{channel}_credit_count'].sum()
            debit_count = df[f'{channel}_debit_count'].sum()

            print(f"\n{channel.upper()} Channel:")
            print(f"Credit Transactions: {int(credit_count):,}")
            print(f"Credit Amount: ${credit_amount:,.2f}")
            print(f"Debit Transactions: {int(debit_count):,}")
            print(f"Debit Amount: ${debit_amount:,.2f}")
            print(f"Net Amount: ${(credit_amount - debit_amount):,.2f}")


def calculate_credit_debit_ratio(folder_path):
    """
    Calculate credit to debit ratio and save to Excel with just customer_id and ratio.
    """
    try:
        # Read the summary CSV file
        input_file = os.path.join(folder_path, "transaction_credit_debit_summary.csv")
        df = pd.read_csv(input_file)

        # Create new dataframe for ratios
        result_df = pd.DataFrame()
        result_df['customer_id'] = df['customer_id']

        # Calculate total credit and debit amounts
        credit_cols = [col for col in df.columns if col.endswith('_credit_amount')]
        debit_cols = [col for col in df.columns if col.endswith('_debit_amount')]

        total_credit = df[credit_cols].sum(axis=1)
        total_debit = df[debit_cols].sum(axis=1)

        # Calculate ratio, set to 1000000000 if debit is zero
        # result_df['credit_debit_ratio'] = np.where(
        #     total_debit == 0,
        #     1000000000,
        #     total_credit / total_debit
        # )
        # Instead of saving the ratio of debit to credit, save the ratio debit to total amount
        # and credit to total amount. This will avoid dealing with case when credit is zero
        
        result_df['credit_total_amount_ratio'] = np.where(
            total_debit + total_credit == 0,
            0,
            total_credit / (total_debit+total_credit)
        )
        
        result_df['debit_total_amount_ratio'] = np.where(
            total_debit + total_credit == 0,
            0,
            total_debit / (total_debit+total_credit)
        )
        
        #result_df["total_credit"] = total_credit
        #result_df["total_debit"] = total_debit

        # Save results to Excel
        output_file = os.path.join(folder_path, "credit_debit_ratio.csv")
        result_df.to_csv(output_file, index=False)

        print(f"\nResults saved to: {output_file}")
        print(f"Total customers: {len(result_df)}")
        print(f"Customers with zero debit: {(total_debit == 0).sum()}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")


if __name__ == "__main__":
    # Set your folder path 
    data_path = os.curdir + '/raw_data'
    folder_path = os.curdir + '/features/Debit_Credit_Ratio'

    # Process files and create summary
    result_df = process_transaction_files(data_path)

    # Save results
    save_results(result_df, folder_path)

    calculate_credit_debit_ratio(folder_path)
