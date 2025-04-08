import pandas as pd

def load_name_mapping(names_parquet):
    """Load the mapping of nconst to primaryName."""
    names_df = pd.read_parquet(names_parquet)
    return dict(zip(names_df['nconst'], names_df['primaryName']))

def load_principal_names(principal_names_parquet):
    """Load the mapping of tconst to principal names."""
    principal_names_df = pd.read_parquet(principal_names_parquet)
    return dict(zip(principal_names_df['tconst'], principal_names_df['principals']))

def replace_crew_with_names(crew_parquet, names_parquet, principal_names_parquet, output_parquet):
    # Load data
    crew_df = pd.read_parquet(crew_parquet)
    name_mapping = load_name_mapping(names_parquet)
    principal_names_mapping = load_principal_names(principal_names_parquet)

    # Replace principals column with actual names
    crew_df['principals'] = crew_df['tconst'].map(principal_names_mapping)

    # Function to map 'directors' or 'writers' (which are comma-separated nconst IDs) to actual names
    def map_crew_to_names(crew_column):
        """Map nconsts in directors or writers to names."""
        return [name_mapping.get(nconst.strip(), nconst.strip()) for nconst in str(crew_column).split(',')] if pd.notna(crew_column) else []

    # Replace directors and writers columns with actual names
    crew_df['directors'] = crew_df['directors'].apply(map_crew_to_names)
    crew_df['writers'] = crew_df['writers'].apply(map_crew_to_names)

    # Combine directors, writers, and principals into one 'all_crew' column
    crew_df['all_crew'] = crew_df.apply(
        lambda row: 
            (row['principals'] if isinstance(row['principals'], list) else []) + 
            row['directors'] + 
            row['writers'], 
        axis=1
    )

    # Save the result
    crew_df.to_parquet(output_parquet, index=False)
    print(f"Processed crew data saved to {output_parquet}")

if __name__ == "__main__":
    replace_crew_with_names(
        "../parquet/merged_with_principal_names.parquet",          # Original dataset with 'directors' and 'writers' columns
        "../parquet/name.basics.parquet",           # Name mapping Parquet file
        "../parquet/principal_names.parquet",      # Principal names Parquet file
        "../parquet/merged_with_all_names.parquet"  # Output file to save the result
    )
