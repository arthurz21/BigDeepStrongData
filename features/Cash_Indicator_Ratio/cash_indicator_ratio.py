import pandas as pd
import os
from pathlib import Path
import numpy as np


def process_abm_file(df):
    """
    Special processing for ABM transactions to handle cash indicators.

    Args:
        df (pd.DataFrame): ABM transactions dataframe

    Returns:
        pd.DataFrame: Customer summary with cash-specific metrics
    """
    # Standard ABM metrics
    standard_metrics = df.groupby('customer_id').agg({
        'customer_id': 'count',  # Count of all transactions
        'amount_cad': lambda x: np.sum(np.abs(x))  # Sum of all transaction amounts
    }).rename(columns={
        'customer_id': 'abm_count',
        'amount_cad': 'abm_amount_cad'
    })

    # Cash-specific metrics
    # Filter for cash transactions (where cash_indicator is True)
    cash_transactions = df[df['cash_indicator'] == True]
    cash_metrics = cash_transactions.groupby('customer_id').agg({
        'customer_id': 'count',  # Count of cash transactions
        'amount_cad': lambda x: np.sum(np.abs(x))  # Sum of cash transaction amounts
    }).rename(columns={
        'customer_id': 'abm_cash_count',
        'amount_cad': 'abm_cash_amount_cad'
    })

    # Merge standard and cash metrics
    combined = pd.merge(standard_metrics, cash_metrics,
                        how='left',
                        left_index=True,
                        right_index=True)

    # Reset index to make customer_id a column
    combined.reset_index(inplace=True)

    # Fill NaN values with 0 (for customers with no cash transactions)
    combined = combined.fillna(0)

    return combined


def process_transaction_files(folder_path):
    """
    Process transaction CSV files to create a consolidated customer-level summary.
    Special handling for ABM transactions to include cash indicators.
    """
    # Initialize empty dataframe for final results
    final_df = None

    # Get all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    for file in csv_files:
        try:
            # Extract channel name from filename
            channel = file.split('.')[0].lower()  # Convert to lowercase for consistency

            # Read the CSV file
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)

            # Special processing for ABM file
            if channel == 'abm':
                customer_summary = process_abm_file(df)
            else:
                # Standard processing for other files
                customer_summary = df.groupby('customer_id').agg({
                    'customer_id': 'count',
                    'amount_cad': lambda x: np.sum(np.abs(x))
                }).rename(columns={
                    'customer_id': f'{channel}_count',
                    'amount_cad': f'{channel}_amount_cad'
                })
                customer_summary.reset_index(inplace=True)

            # Merge with final_df if it exists, otherwise initialize it
            if final_df is None:
                final_df = customer_summary
            else:
                final_df = pd.merge(final_df, customer_summary,
                                    on='customer_id',
                                    how='outer')

            print(f"Processed: {file}")

        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
            print(f"Columns in the current file: {df.columns.tolist()}")
            if final_df is not None:
                print(f"Columns in final_df: {final_df.columns.tolist()}")

    # Fill NaN values with 0
    if final_df is not None:
        final_df = final_df.fillna(0)

        # Sort columns alphabetically after customer_id
        cols = ['customer_id'] + sorted([col for col in final_df.columns if col != 'customer_id'])
        final_df = final_df[cols]

        # Sort by customer_id
        final_df = final_df.sort_values('customer_id')

    return final_df


def save_results(df, output_path, filename="transaction_summary.csv"):
    """
    Save the results to a CSV file and print summary statistics.
    """
    if df is not None:
        # Create output directory if it doesn't exist
        Path(output_path).mkdir(parents=True, exist_ok=True)

        # Save to CSV
        output_file = os.path.join(output_path, filename)
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")

        # Print summary statistics
        print("\nSummary of results:")
        print(f"Total unique customers: {len(df)}")
        for col in df.columns:
            if col != 'customer_id':
                print(f"{col}:")
                if 'count' in col:
                    print(f"  Total transactions: {int(df[col].sum())}")
                else:
                    print(f"  Total amount: ${df[col].sum():,.2f}")


def calculate_transaction_ratios(folder_path):
    """
    Calculate ABM transaction ratios (count and amount) compared to all transactions.

    Args:
        folder_path (str): Path to the folder containing transaction summary CSV
    """
    try:
        # Find the transaction summary CSV in the folder
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        input_file = os.path.join(folder_path, csv_files[0])  # Assuming it's the first CSV file

        # Create output file path in the same folder
        output_file = os.path.join(folder_path, 'abm_ratio_analysis.xlsx')

        # Read the transaction summary file
        df = pd.read_csv(input_file)

        # Calculate total counts (excluding cash_count)
        count_columns = [col for col in df.columns if col.endswith('_count') and col != 'cash_count']
        total_count = df[count_columns].sum(axis=1)

        # Calculate total amounts (excluding cash_amount_cad)
        amount_columns = [col for col in df.columns if col.endswith('_amount_cad') and col != 'cash_amount_cad']
        total_amount = df[amount_columns].sum(axis=1)

        # Create new dataframe with ratios
        result_df = pd.DataFrame()
        result_df['customer_id'] = df['customer_id']

        # Calculate ratios
        result_df['abm_count_ratio'] = df['abm_count'] / total_count
        result_df['abm_amount_ratio'] = df['abm_amount_cad'] / total_amount

        # Add original counts and amounts for reference
        #result_df['abm_count'] = df['abm_count']
        #result_df['total_transactions'] = total_count
        #result_df['abm_amount'] = df['abm_amount_cad']
        #result_df['total_amount'] = total_amount

        # Convert ratios to percentages
        #result_df['abm_count_percentage'] = result_df['abm_count_ratio'] * 100
        #result_df['abm_amount_percentage'] = result_df['abm_amount_ratio'] * 100

        # Calculate summary statistics
        summary_stats = pd.DataFrame({
            'Metric': [
                'Overall ABM Count Ratio',
                'Overall ABM Amount Ratio',
                'Total ABM Transactions',
                'Total All Transactions',
                'Total ABM Amount',
                'Total All Amount'
            ],
            'Value': [
                df['abm_count'].sum() / total_count.sum() * 100,
                df['abm_amount_cad'].sum() / total_amount.sum() * 100,
                df['abm_count'].sum(),
                total_count.sum(),
                df['abm_amount_cad'].sum(),
                total_amount.sum()
            ]
        })

        # Create Excel writer object
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write detailed ratios to first sheet
            result_df.to_excel(writer, sheet_name='Customer Ratios', index=False)

            # Write summary statistics to second sheet
            summary_stats.to_excel(writer, sheet_name='Summary Statistics', index=False)

            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for idx, col in enumerate(result_df.columns):
                    max_length = max(
                        result_df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = max_length

        print(f"\nAnalysis completed and saved to: {output_file}")

        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Overall ABM Transaction Ratio: {summary_stats.iloc[0]['Value']:.2f}%")
        print(f"Overall ABM Amount Ratio: {summary_stats.iloc[1]['Value']:.2f}%")
        print(f"Total ABM Transactions: {int(summary_stats.iloc[2]['Value']):,}")
        print(f"Total All Transactions: {int(summary_stats.iloc[3]['Value']):,}")
        print(f"Total ABM Amount: ${summary_stats.iloc[4]['Value']:,.2f}")
        print(f"Total All Amount: ${summary_stats.iloc[5]['Value']:,.2f}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Files in directory: {os.listdir(folder_path)}")

if __name__ == "__main__":
    # Set your paths here
    folder_path = r"C:\Users\arthu\Downloads\ML_comp\processed_data"
    output_path = r"C:\Users\arthu\Downloads\ML_comp\section_2"

    # Process files and create summary
    result_df = process_transaction_files(folder_path)

    # Save results
    save_results(result_df, output_path)

    # Set your file paths
    folder_path = r"C:\Users\arthu\Downloads\ML_comp\section_2"

    # Run the analysis
    calculate_transaction_ratios(folder_path)