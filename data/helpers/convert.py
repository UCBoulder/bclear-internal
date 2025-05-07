import os
import argparse
import polars as pl

def convert_csv_to_parquet(input_folder, output_folder):
    # Ensure the output directory exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Iterate over all CSV files in the input folder
    for file in os.listdir(input_folder):
        if file.lower().endswith('.csv'):
            csv_path = os.path.join(input_folder, file)
            print(f"Processing {csv_path}...")
            
            # Create a lazy DataFrame with streaming enabled
            lazy_df = pl.scan_csv(csv_path, ignore_errors=True, truncate_ragged_lines=True, infer_schema_length=1000)
            
            # Collect the data in streaming mode, reducing memory usage
            #df = lazy_df.collect(streaming=True)
            
            # Define the output Parquet file path (one file per CSV)
            name = file.split('_')
            if len(name)==2:
                new_name = file.replace('.csv', '.parquet')
            else:
                new_name = name[0] + '_' + name[1] + '.parquet'
            
            parquet_file = os.path.join(output_folder, new_name)
            
            # Write the DataFrame to a Parquet file
            lazy_df.sink_parquet(parquet_file)
            print(f"Finished writing Parquet file to {parquet_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Convert large CSV files to Parquet using Polars with streaming execution."
    )
    parser.add_argument("--input", required=True, help="Path to the folder containing CSV files.")
    parser.add_argument("--output", required=True, help="Path to the output folder for Parquet files.")
    args = parser.parse_args()

    convert_csv_to_parquet(args.input, args.output)
