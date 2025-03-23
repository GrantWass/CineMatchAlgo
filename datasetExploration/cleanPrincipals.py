import pandas as pd
import json

def clean_dataset(input_file, output_file, chunk_size=100000):    
    valid_categories = {'director', 'producer', 'cinematographer', 'composer', 'writer', 'actor', 'actress'}
    valid_jobs = {'producer', 'director of photography', 'screenplay', 'story', 'writer', 'poem', 'composer', 'play', 'novel'}

    chunks = []
    for chunk in pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"], chunksize=chunk_size):
        chunk = chunk[chunk['category'].isin(valid_categories)]
        chunk = chunk[chunk['job'].isin(valid_jobs) | chunk['job'].isnull()]
        chunk = chunk[['tconst', 'nconst', 'category', 'characters']]
        
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

        chunk['characters'] = chunk['characters'].apply(extract_character)
    
        chunk = chunk.groupby('tconst').apply(lambda x: {
            "tconst": x.name,
            "principals": x.apply(lambda row: {
                "characterName": row['characters'],
                "nconst": row['nconst'],
                "category": row['category']
            }, axis=1).tolist()
        }).reset_index(drop=True)

        print(chunk.iloc[0]['principals'])
        print(chunk.head())    
        print(chunk.dtypes)

        chunks.append(chunk)

    final_data = pd.concat(chunks, ignore_index=True)

    print(final_data.shape)
    print(chunk.isnull().sum())

    final_data.to_parquet(output_file, engine="pyarrow", compression="snappy")

if __name__ == "__main__":
    input_file = 'title.principals.tsv'
    output_file = 'parquet/title.principals.parquet'
    
    clean_dataset(input_file, output_file)
