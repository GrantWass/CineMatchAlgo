import pandas as pd
import json

def process_akas(df):
    df = df[['titleId', 'title']].dropna(subset=['titleId', 'title'])

    # creates comma separeted list of titles for each titleId
    return df.groupby('titleId')['title'].apply(lambda x: ', '.join(x)).reset_index().rename(columns={'title': 'titles'})

def process_basics(df):
    # drop if primaryTitle is null
    df = df.dropna(subset=['primaryTitle'])

    # keep only relevant movies (this could be adjusted to include shows, etc.)
    df = df[df['titleType'].isin(['movie'])]

    # convert runtimeMinutes to numeric (some are strings)
    df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
    return df

def process_crew(df):
    # drops rows where both directors and writers are null
    return df.dropna(subset=['directors', 'writers'], how='all')

# this could really be a graph database if we had more time
def process_principals(df):
        valid_categories = {'director', 'producer', 'cinematographer', 'composer', 'writer', 'actor', 'actress'}
        valid_jobs = {'producer', 'director of photography', 'screenplay', 'story', 'writer', 'poem', 'composer', 'play', 'novel'}
        df = df[df['category'].isin(valid_categories)]
        df = df[df['job'].isin(valid_jobs) | df['job'].isnull()]
        df = df[['tconst', 'nconst', 'category', 'characters']]

        def extract_character(characters):
            # this should get first character from list of characters (weird logic bc it is a list of lists)
            if pd.notna(characters):
                try:
                    parsed = json.loads(characters)
                    if isinstance(parsed, list) and parsed:
                        return parsed[0] if isinstance(parsed[0], str) else parsed[0][0]
                except json.JSONDecodeError:
                    return None
            return None

        df['characters'] = df['characters'].apply(extract_character)
    
        df = df.groupby('tconst').apply(lambda x: {
            "tconst": x.name,
            "prinncipals": x.apply(lambda row: {
                "characterName": row['characters'],
                "nconst": row['nconst'],
                "category": row['category']
            }, axis=1).tolist()
        }).reset_index(drop=True)

        return df

# these didn't need to be cleaned, just here as a placeholder
def process_ratings(df):
    return df


def clean_dataset(dataset_name, process_function, chunk_size=None):
    input_file = f"{dataset_name}.tsv"
    output_file = f"parquet/{dataset_name}.parquet"

    # read with chunking if needed (for large datasets like 'akas' my mac doesn't have enough memory)
    if chunk_size:
        aggregated_data = pd.DataFrame()
        # currently this functionality is for the akas dataset, need to adjust for others
        for chunk in pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"], chunksize=chunk_size):
            processed_chunk = process_function(chunk)
            aggregated_data = pd.concat([aggregated_data, processed_chunk])
            print(f"Processed chunk: {len(processed_chunk)} rows")

        if dataset_name == "title.akas":
            # creates comma separeted list of titles for each titleId
            aggregated_data = aggregated_data.groupby('titleId')['titles'].apply(lambda x: ', '.join(x)).reset_index()
        else:
            aggregated_data = aggregated_data.drop_duplicates()

    else:
        # read the entire dataset
        df = pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"])
        aggregated_data = process_function(df)

    aggregated_data.to_parquet(output_file, engine="pyarrow", compression="snappy")
    print(f"Processed '{dataset_name}': {len(aggregated_data)} rows")

if __name__ == "__main__":
    clean_dataset("title.akas", process_akas, chunk_size=100000)
    clean_dataset("title.basics", process_basics)
    clean_dataset("title.crew", process_crew)
    clean_dataset("title.principals", process_principals, chunk_size=100000)
    clean_dataset("title.ratings", process_ratings)
