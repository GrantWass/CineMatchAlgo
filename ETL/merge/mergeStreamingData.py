import pandas as pd
import re

def normalize_title(title):
    """Remove spaces and non-alphanumeric characters to match formats."""
    return re.sub(r'[^a-zA-Z0-9]', '', str(title))

def merge_streaming_data(movies_parquet, streaming_csv, output_parquet):
    # Load datasets
    movies_df = pd.read_parquet(movies_parquet)
    streaming_df = pd.read_csv(streaming_csv)

    # Normalize titles for matching
    movies_df['normalizedTitle'] = movies_df['primaryTitle'].apply(normalize_title)
    streaming_df['normalizedTitle'] = streaming_df['Title'].apply(normalize_title)

    # Prepare streaming services column
    def get_streaming_services(row):
        services = []
        if row.get('Netflix', 0) == 1:
            services.append('Netflix')
        if row.get('Hulu', 0) == 1:
            services.append('Hulu')
        if row.get('Prime Video', 0) == 1:
            services.append('Prime Video')
        if row.get('Disney+', 0) == 1:
            services.append('Disney+')
        return ', '.join(services) if services else None

    streaming_df['StreamingServices'] = streaming_df.apply(get_streaming_services, axis=1)

    # Merge datasets on normalized titles
    merged_df = movies_df.merge(streaming_df[['normalizedTitle', 'StreamingServices']], on='normalizedTitle', how='left')

    # Calculate merge statistics
    total_movies = len(movies_df)
    total_streaming = len(streaming_df)
    total_merged = len(merged_df)
    merge_percentage = (total_merged / min(total_movies, total_streaming)) * 100

    print(f"Total Movies entries: {total_movies}")
    print(f"Total Streaming entries: {total_streaming}")
    print(f"Total Merged entries: {total_merged}")
    print(f"Merge Success Rate: {merge_percentage:.2f}%")

    # Drop the extra normalization column
    merged_df = merged_df.drop(columns=['normalizedTitle'])

    # Save merged data to a new Parquet file
    merged_df.to_parquet(output_parquet, index=False)
    print(f"Merged dataset saved to {output_parquet}")

if __name__ == "__main__":
    merge_streaming_data("../parquet/merged_imdb_tropes.parquet", "../MoviesOnStreamingPlatforms.csv", "../parquet/merged_streaming_services.parquet")
