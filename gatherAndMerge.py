import pandas as pd

def load_parquet(filepath):
    return pd.read_parquet(filepath)

def merge_imdb_data(basics_file, akas_file, crew_file, principals_file, ratings_file):
    basics_df = load_parquet(basics_file)
    # akas_df = load_parquet(akas_file)
    crew_df = load_parquet(crew_file)
    # principals_df = load_parquet(principals_file)
    ratings_df = load_parquet(ratings_file)
    
    merged_df = basics_df.merge(ratings_df, on='tconst', how='left')
    # merged_df = merged_df.merge(akas_df, left_on='tconst', right_on='titleId', how='left').drop(columns=['titleId'])
    merged_df = merged_df.merge(crew_df, on='tconst', how='left')
    # merged_df = merged_df.merge(principals_df, on='tconst', how='left')
    
    return merged_df

if __name__ == "__main__":
    combined_df = merge_imdb_data(
        'parquet/title.basics.parquet', 
        'parquet/title.akas.parquet', 
        'parquet/title.crew.parquet', 
        'parquet/title.principals.parquet', 
        'parquet/title.ratings.parquet'
    )
    
    print(combined_df.head())
    combined_df.to_parquet('parquet/combined_imdb_data.parquet', engine='pyarrow', compression='snappy')
