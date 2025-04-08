import pandas as pd
import numpy as np

def replace_crew_with_names(crew_parquet, names_parquet, principal_names_parquet, output_parquet):
    # Load data
    crew_df = pd.read_parquet(crew_parquet)

    # Ensure 'principals' and 'all_crew' are numpy arrays, and convert to lists if necessary
    def safe_convert_to_list(arr):
        if isinstance(arr, np.ndarray):  # If it's a numpy ndarray, convert to a list
            return arr.tolist()
        elif isinstance(arr, list):  # If it's already a list, return it
            return arr
        else:
            return []  # Return an empty list if it's neither a numpy array nor a list

    crew_df['principals'] = crew_df['principals'].apply(safe_convert_to_list)
    crew_df['all_crew'] = crew_df['all_crew'].apply(safe_convert_to_list)

    # Combine principals and all_crew into one 'AllPeople' column
    def combine_crew(row):
        # Concatenate the lists (which are now safe to handle)
        return list(set(row['principals'] + row['all_crew']))

    # Apply the combine function row-wise
    crew_df['AllPeople'] = crew_df.apply(combine_crew, axis=1)

    # Save the result
    crew_df.to_parquet(output_parquet, index=False)
    print(f"Processed crew data saved to {output_parquet}")


if __name__ == "__main__":
    replace_crew_with_names(
        "../parquet/merged_with_all_names.parquet",          # Original dataset with 'directors' and 'writers' columns
        "../parquet/name.basics.parquet",           # Name mapping Parquet file
        "../parquet/principal_names.parquet",      # Principal names Parquet file
        "../parquet/final_combined_dataset.parquet"  # Output file to save the result
    )
