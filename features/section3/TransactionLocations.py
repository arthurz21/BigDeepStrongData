import pandas as pd
import os
from pathlib import Path


class CustomerCityAnalyzer:
    def __init__(self, input_folder, output_folder):
        """
        Initialize the analyzer with input and output paths

        Parameters:
        input_folder (str): Path to folder containing transaction CSV files
        output_folder (str): Path where output files will be saved
        """
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.customer_cities = {}
        self.files_processed = []
        self.files_skipped = []

        # Create output folder if it doesn't exist
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def get_city_column(self, df):
        """Find the city column in the dataframe"""
        possible_city_cols = ['city', 'CITY', 'City']
        for col in df.columns:
            if col.lower() == 'city':
                return col
        return None

    def process_file(self, file_path):
        """Process a single transaction file"""
        try:
            print(f"\nProcessing file: {file_path}")
            df = pd.read_csv(file_path)

            # Find city column
            city_col = self.get_city_column(df)
            if city_col is None:
                print(f"No city column found in {file_path}, skipping...")
                self.files_skipped.append(file_path.name)
                return

            # Ensure we have customer_id
            if 'customer_id' not in df.columns:
                print(f"No customer_id column found in {file_path}, skipping...")
                self.files_skipped.append(file_path.name)
                return

            # Process cities
            print(f"Found city column: {city_col}")

            # Group by customer and get unique cities (normalized to lowercase)
            customer_cities = df.groupby('customer_id')[city_col].agg(
                lambda x: set(str(city).lower().strip() for city in x if pd.notna(city))
            )

            # Update the main dictionary
            for customer_id, cities in customer_cities.items():
                if customer_id not in self.customer_cities:
                    self.customer_cities[customer_id] = set()
                self.customer_cities[customer_id].update(cities)

            self.files_processed.append(file_path.name)
            print(f"Processed {len(df)} transactions")

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            self.files_skipped.append(file_path.name)

    def create_summary(self):
        """Create summary file with customer city counts"""
        try:
            # Create summary data
            summary_data = []
            for customer_id, cities in self.customer_cities.items():
                summary_data.append({
                    'customer_id': customer_id,
                    'unique_cities': len(cities)#,
                    #'cities_list': '|'.join(sorted(cities))
                })

            # Convert to DataFrame and sort by unique city count
            summary_df = pd.DataFrame(summary_data)
            summary_df = summary_df.sort_values('customer_id', ascending=True)

            # Save to file
            output_file = self.output_folder / 'customer_city_summary.csv'
            summary_df.to_csv(output_file, index=False)

            # Create processing log
            log_file = self.output_folder / 'processing_log.txt'
            with open(log_file, 'w') as f:
                f.write("Processing Summary\n")
                f.write("=================\n\n")
                f.write(f"Total customers processed: {len(summary_df)}\n")
                f.write(f"Files processed: {len(self.files_processed)}\n")
                f.write(f"Files skipped: {len(self.files_skipped)}\n\n")

                f.write("Files Processed:\n")
                for file in self.files_processed:
                    f.write(f"- {file}\n")

                f.write("\nFiles Skipped:\n")
                for file in self.files_skipped:
                    f.write(f"- {file}\n")

                f.write("\nDistribution of unique cities per customer:\n")
                f.write(str(summary_df['unique_cities'].describe()))

            print("\nSummary Statistics:")
            print(f"Total customers processed: {len(summary_df)}")
            print(f"Files processed: {len(self.files_processed)}")
            print(f"Files skipped: {len(self.files_skipped)}")
            print(f"\nDistribution of unique cities per customer:")
            print(summary_df['unique_cities'].describe())
            print(f"\nFirst few rows of summary:")
            print(summary_df.head())
            print(f"\nOutput files saved to: {self.output_folder}")

        except Exception as e:
            print(f"Error creating summary: {str(e)}")
            raise


def main():
    # Example usage with input and output folders
    input_folder = r"C:\Users\arthu\Downloads\ML_comp\section_3"  # Replace with your input folder path
    output_folder = r"C:\Users\arthu\Downloads\ML_comp"  # Replace with your output folder path

    analyzer = CustomerCityAnalyzer(input_folder, output_folder)

    # Get all CSV files in input directory
    csv_files = list(Path(input_folder).glob('*.csv'))
    print(f"Found {len(csv_files)} CSV files in {input_folder}")

    # Process each file
    for file_path in csv_files:
        analyzer.process_file(file_path)

    # Create summary
    analyzer.create_summary()


if __name__ == "__main__":
    main()