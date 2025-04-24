import pandas as pd
import json
import concurrent.futures

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
        valid_categories = {'producer','composer', 'actor', 'actress'}
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

        df = df.groupby('tconst').apply(lambda x: x[['nconst', 'category', 'characters']].to_dict(orient='records'), include_groups=False).reset_index(name='principals')
    
        return df

# these didn't need to be cleaned, just here as a placeholder
def process_ratings(df):
    return df


def clean_dataset(dataset_name, process_function, chunk_size=None):
    input_file = f"{dataset_name}.tsv"
    output_file = f"parquet/{dataset_name}.parquet"

    def process_chunk(chunk):
        return process_function(chunk)

    if chunk_size:
        # process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for chunk in pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"], chunksize=chunk_size):
                print(f"Processing chunk")
                futures.append(executor.submit(process_chunk, chunk))

            # collect the processed chunks
            processed_chunks = []
            for future in concurrent.futures.as_completed(futures):
                processed_chunks.append(future.result())
                print(f"Processed chunk")

            aggregated_data = pd.concat(processed_chunks, ignore_index=True)

        if dataset_name == "title.akas":
            aggregated_data = aggregated_data.groupby('titleId')['titles'].apply(lambda x: ', '.join(x)).reset_index()

    else:
        df = pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"])
        aggregated_data = process_function(df)

    if isinstance(aggregated_data, pd.Series):
        aggregated_data = aggregated_data.to_frame()

    aggregated_data.to_parquet(output_file, engine="pyarrow", compression="snappy")
    print(f"Processed '{dataset_name}': {len(aggregated_data)} rows")

if __name__ == "__main__":
    clean_dataset("title.principals", process_principals, chunk_size=500000)
    clean_dataset("title.basics", process_basics)
    clean_dataset("title.crew", process_crew)
    clean_dataset("title.akas", process_akas, chunk_size=100000)
    clean_dataset("title.ratings", process_ratings)
