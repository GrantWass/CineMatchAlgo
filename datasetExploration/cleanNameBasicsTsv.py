import pandas as pd

def convert_tsv_to_parquet(input_tsv, output_parquet):
    # Read the TSV file
    df = pd.read_csv(input_tsv, delimiter='\t')
    
    # Save to Parquet
    df.to_parquet(output_parquet, index=False)
    print(f"TSV file '{input_tsv}' successfully converted to Parquet as '{output_parquet}'.")

if __name__ == "__main__":
    convert_tsv_to_parquet("../name.basics.tsv", "../parquet/name.basics.parquet")

