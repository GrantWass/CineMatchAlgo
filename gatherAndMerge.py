import pandas as pd

def load_tsv(filepath):
    return pd.read_csv(filepath, sep='\t', low_memory=False, na_values=['\\N'])

def merge_imdb_data(basics_file, akas_file, crew_file, principals_file, ratings_file):
    # Going to try to use parquet files instead of tsv
    basics_df = load_tsv(basics_file)
    akas_df = load_tsv(akas_file)
    crew_df = load_tsv(crew_file)
    principals_df = load_tsv(principals_file)
    ratings_df = load_tsv(ratings_file)
    
    merged_df = basics_df.merge(ratings_df, on='tconst', how='left')
    
    akas_df = akas_df[['title', 'region', 'language', 'types', 'isOriginalTitle', 'titleId']]
    merged_df = merged_df.merge(akas_df, left_on='tconst', right_on='titleId', how='left').drop(columns=['titleId'])
    
    crew_df = crew_df[['tconst', 'directors', 'writers']]
    merged_df = merged_df.merge(crew_df, on='tconst', how='left')
    
    principals_df = principals_df[['tconst', 'nconst', 'category', 'characters']]
    merged_df = merged_df.merge(principals_df, on='tconst', how='left')
    
    return merged_df

if __name__ == "__main__":
    combined_df = merge_imdb_data(
        'title.basics.tsv', 
        'title.akas.tsv', 
        'title.crew.tsv', 
        'title.principals.tsv', 
        'title.ratings.tsv'
    )
    
    print(combined_df.head())
    combined_df.to_csv('combined_imdb_data.csv', index=False)
