import pandas as pd

def merge_principals(crew_parquet, new_principals_parquet, output_parquet):
    # Load the data
    crew_df = pd.read_parquet(crew_parquet)
    new_principals_df = pd.read_parquet(new_principals_parquet)

    # Merge the data on the 'tconst' column
    merged_df = pd.merge(crew_df, new_principals_df[['tconst', 'principals']], on='tconst', how='left')

    # Replace the 'principals' column in crew_df with the new 'principals' from new_principals_df
    merged_df['principals'] = merged_df['principals_y']
    
    # Drop the extra 'principals_y' column that was created during the merge
    merged_df = merged_df.drop(columns=['principals_y'])

    # Save the result to a new Parquet file
    merged_df.to_parquet(output_parquet, index=False)
    print(f"✅ Principals column has been replaced and saved to {output_parquet}")

if __name__ == "__main__":
    merge_principals(
        "../parquet/merged_streaming_services.parquet",      # Original dataset with the 'principals' column
        "../parquet/principal_names.parquet",    # New dataset with the updated 'principals' column
        "../parquet/merged_with_principal_names.parquet"       # Output file to save the result
    )
