import pandas as pd
import re

def normalize_title(title):
    """Remove spaces and non-alphanumeric characters to match the tropes dataset format."""
    return re.sub(r'[^a-zA-Z0-9]', '', title)

def merge_imdb_tropes(imdb_parquet, tropes_csv, output_parquet):
    # Load datasets
    imdb_df = pd.read_parquet(imdb_parquet)
    tropes_df = pd.read_csv(tropes_csv)
    
    # Normalize titles for matching
    imdb_df['normalizedTitle'] = imdb_df['primaryTitle'].apply(normalize_title)
    tropes_df['normalizedTitle'] = tropes_df['Title'].apply(normalize_title)
    
    # Merge datasets on normalized titles
    merged_df = imdb_df.merge(tropes_df, on='normalizedTitle', how='inner')
    
    # Calculate merge statistics
    total_imdb = len(imdb_df)
    total_tropes = len(tropes_df)
    total_merged = len(merged_df)
    
    print(f"Total IMDb entries: {total_imdb}")
    print(f"Total Tropes entries: {total_tropes}")
    print(f"Total Merged entries: {total_merged}")
    
    # Drop the extra normalization column
    merged_df = merged_df.drop(columns=['normalizedTitle'])
    
    # Save merged data to a new Parquet file
    merged_df.to_parquet(output_parquet, index=False)
    print(f"Merged dataset saved to {output_parquet}")

if __name__ == "__main__":
    merge_imdb_tropes("../parquet/combined_imdb_data.parquet", "../cleaned_film_tropes.csv", "../parquet/merged_imdb_tropes.parquet")
