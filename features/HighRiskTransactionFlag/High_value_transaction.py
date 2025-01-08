import pandas as pd
import numpy as np
import os
from pathlib import Path


class TransactionAnalyzer:
    def __init__(self, kyc_file_path):
        """Initialize the analyzer with the KYC data file"""
        try:
            self.kyc_data = pd.read_csv(kyc_file_path)
            self.kyc_data['industry_code'] = self.kyc_data['industry_code'].fillna('UNKNOWN').astype(str)
            print(f"Loaded KYC data with {len(self.kyc_data)} records")
        except Exception as e:
            raise Exception(f"Error loading KYC file: {str(e)}")

        self.transaction_data = {}

    def get_amount_column(self, df):
        """Determine the amount column name"""
        amount_columns = ['amount', 'amount_cad', 'AMOUNT', 'AMOUNT_CAD']
        for col in amount_columns:
            if col in df.columns:
                return col
        raise ValueError(f"No amount column found. Expected one of: {amount_columns}")

    def load_transaction_file(self, transaction_type, file_path):
        """Load and prepare transaction data"""
        try:
            df = pd.read_csv(file_path)
            amount_col = self.get_amount_column(df)
            print(f"Using '{amount_col}' as amount column for {transaction_type}")

            # Convert to numeric, take absolute value, and exclude zeros
            df['amount'] = pd.to_numeric(df[amount_col].astype(str).str.replace(',', ''), errors='coerce').abs()
            df = df.dropna(subset=['amount'])
            df = df[df['amount'] > 0]  # Exclude zero values

            self.transaction_data[transaction_type] = df
            print(f"Loaded {len(df)} valid transactions for type {transaction_type}")

        except Exception as e:
            raise Exception(f"Error loading transaction file {transaction_type}: {str(e)}")

    def normalize_customer_transactions(self, transactions_df):
        """Normalize transaction amounts for each customer more efficiently"""
        # Calculate sums using vectorized operations
        customer_sums = transactions_df.groupby('customer_id')['amount'].sum()

        # Join back to original dataframe (faster than apply)
        normalized_df = transactions_df.copy()
        normalized_df['customer_sum'] = normalized_df['customer_id'].map(customer_sums)
        normalized_df['normalized_amount'] = normalized_df['amount'] / normalized_df['customer_sum']
        normalized_df.drop('customer_sum', axis=1, inplace=True)

        return normalized_df

    def process_transactions(self, output_dir):
        """Process transactions and generate analysis files"""
        os.makedirs(output_dir, exist_ok=True)
        processing_summary = []

        # Store all high value transactions for final summary
        all_high_value_results = {
            trans_type: {} for trans_type in self.transaction_data.keys()
        }

        # Get list of industries once
        known_industries = self.kyc_data['industry_code'].unique()

        # Create customer to industry mapping for faster lookup
        customer_industry_map = dict(zip(self.kyc_data['customer_id'], self.kyc_data['industry_code']))

        print(f"\nProcessing {len(known_industries)} industries...")
        total_industries = len(known_industries)

        for idx, industry in enumerate(known_industries, 1):
            print(f"\nProcessing industry {idx}/{total_industries}: {industry}")

            # Get customers for this industry
            industry_customers = set(self.kyc_data[self.kyc_data['industry_code'] == industry]['customer_id'])

            for trans_type, trans_df in self.transaction_data.items():
                try:
                    # Filter transactions more efficiently using isin
                    industry_transactions = trans_df[trans_df['customer_id'].isin(industry_customers)].copy()

                    if len(industry_transactions) == 0:
                        processing_summary.append({
                            'industry': industry,
                            'transaction_type': trans_type,
                            'status': 'Skipped - No transactions found'
                        })
                        continue

                    print(f"  Processing {trans_type}: {len(industry_transactions)} transactions")

                    # Normalize transactions
                    normalized_transactions = self.normalize_customer_transactions(industry_transactions)

                    # Calculate 90th percentile
                    percentile_90 = normalized_transactions['normalized_amount'].quantile(0.9)

                    # Find high-value transactions efficiently
                    high_value_mask = normalized_transactions['normalized_amount'] > percentile_90
                    high_value_counts = normalized_transactions[high_value_mask]['customer_id'].value_counts()

                    # Store results efficiently
                    for customer_id, count in high_value_counts.items():
                        all_high_value_results[trans_type][customer_id] = \
                            all_high_value_results[trans_type].get(customer_id, 0) + count

                    # Store results for final summary
                    for customer_id in high_value_counts.index:
                        if customer_id not in all_high_value_results[trans_type]:
                            all_high_value_results[trans_type][customer_id] = 0
                        all_high_value_results[trans_type][customer_id] += high_value_counts[customer_id]

                    # Create individual industry summary
                    summary = pd.DataFrame({
                        'customer_id': high_value_counts.index,
                        'transactions_above_90th': high_value_counts.values,
                        'total_transactions': normalized_transactions['customer_id'].value_counts()[
                            high_value_counts.index].values
                    })

                    # Add additional info
                    summary['normalized_90th_percentile'] = percentile_90
                    summary['high_value_transaction_percentage'] = (
                            summary['transactions_above_90th'] / summary['total_transactions'] * 100
                    ).round(2)
                    summary['industry_code'] = industry
                    summary['total_amount'] = industry_transactions.groupby('customer_id')['amount'].sum()[
                        high_value_counts.index].values

                    # Save individual industry results
                    safe_industry = str(industry).replace(' ', '_').replace('/', '_').replace('\\', '_')
                    output_file = f"{output_dir}/industry_{safe_industry}_{trans_type}_analysis.csv"
                    summary.to_csv(output_file, index=False)

                    processing_summary.append({
                        'industry': industry,
                        'transaction_type': trans_type,
                        'status': 'Success',
                        'transactions_processed': len(normalized_transactions),
                        'customers_with_high_value_trans': len(summary),
                        'normalized_90th_percentile': percentile_90
                    })

                except Exception as e:
                    processing_summary.append({
                        'industry': industry,
                        'transaction_type': trans_type,
                        'status': f'Error - {str(e)}'
                    })

        # Save processing summary
        pd.DataFrame(processing_summary).to_csv(
            f"{output_dir}/processing_summary.csv",
            index=False
        )

        # Create final customer summary
        print("\nStarting to create final customer summary...")
        print(f"Number of transaction types with high value results: {len(all_high_value_results)}")
        for t_type, results in all_high_value_results.items():
            print(f"Transaction type {t_type}: {len(results)} customers with high value transactions")
        self.create_customer_summary(all_high_value_results, output_dir)

    def create_customer_summary(self, all_high_value_results, output_dir):
        """Create summary of high value transactions across all industries more efficiently"""
        try:
            print("\nStarting to create customer summary...")
            all_trans_types = list(self.transaction_data.keys())
            print(f"Found {len(all_trans_types)} transaction types: {all_trans_types}")

            # Convert the nested dict structure to a DataFrame directly
            summary_rows = []
            for trans_type, customer_counts in all_high_value_results.items():
                if customer_counts:  # Only process if there are results
                    df = pd.DataFrame.from_dict(customer_counts, orient='index',
                                                columns=[f'{trans_type}_high_value_count'])
                    summary_rows.append(df)

            # Combine all transaction types
            if summary_rows:
                summary_df = pd.concat(summary_rows, axis=1).fillna(0)
                summary_df.index.name = 'customer_id'
                summary_df.reset_index(inplace=True)
            else:
                print("No high value transactions found!")
                return

            # Add total transaction counts efficiently
            print("Calculating total transaction counts...")
            for trans_type in all_trans_types:
                # Count transactions per customer using value_counts
                total_counts = self.transaction_data[trans_type]['customer_id'].value_counts()
                #summary_df[f'{trans_type}_total_count'] = summary_df['customer_id'].map(total_counts).fillna(0)

            # Calculate total high value count
            high_value_cols = [col for col in summary_df.columns if col.endswith('_high_value_count')]
            #summary_df['total_high_value_count'] = summary_df[high_value_cols].sum(axis=1)

            # Sort by total high value counts
            #summary_df = summary_df.sort_values('total_high_value_count', ascending=False)

            # Save summary
            output_file = f"{output_dir}/customer_high_value_summary.csv"
            summary_df.to_csv(output_file, index=False)
            print(f"\nCreated customer summary with {len(summary_df)} customers")
            print(f"Transaction types included: {', '.join(all_trans_types)}")
            print(f"Summary file saved to: {output_file}")
            print("\nFirst few rows of summary:")
            print(summary_df.head())

        except Exception as e:
            print(f"\nError creating customer summary: {str(e)}")
            print("Debug information:")
            print(f"Transaction types: {all_trans_types}")
            print(f"All high value results keys: {list(all_high_value_results.keys())}")
            raise


def main():
    analyzer = TransactionAnalyzer(r"C:\Users\arthu\Downloads\ML_comp\section_6_data\kyc.csv")

    transaction_files = {
        'abm': r"C:\Users\arthu\Downloads\ML_comp\section_6_data\abm.csv",
        'emt': r"C:\Users\arthu\Downloads\ML_comp\section_6_data\emt.csv",
        'card': r"C:\Users\arthu\Downloads\ML_comp\section_6_data\card.csv",
        'wire': r"C:\Users\arthu\Downloads\ML_comp\section_6_data\wire.csv",
        'eft': r"C:\Users\arthu\Downloads\ML_comp\section_6_data\eft.csv",
        'cheque': r"C:\Users\arthu\Downloads\ML_comp\section_6_data\cheque.csv",
    }
    for trans_type, file_path in transaction_files.items():
        analyzer.load_transaction_file(trans_type, file_path)

    analyzer.process_transactions(r"C:\Users\arthu\Downloads\ML_comp\section_6_data")


if __name__ == "__main__":
    main()