import pandas as pd

def process_chunk(chunk):
    chunk = chunk[['titleId', 'title']]
    chunk = chunk.dropna(subset=['titleId', 'title']) 

    chunk_grouped = chunk.groupby('titleId')['title'].apply(lambda x: ', '.join(x)).reset_index()

    return chunk_grouped

def clean_dataset(input_file, output_file, chunk_size=100000):
    aggregated_data = {}

    for chunk in pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"], chunksize=chunk_size):
        chunk_grouped = process_chunk(chunk)

        for index, row in chunk_grouped.iterrows():
            title_id = row['titleId']
            title_list = row['title']

            if title_id in aggregated_data:
                aggregated_data[title_id] += ', ' + title_list
            else:
                aggregated_data[title_id] = title_list

    final_df = pd.DataFrame(list(aggregated_data.items()), columns=['titleId', 'titles'])

    print(len(final_df))
    print(final_df.head())

    final_df.to_parquet(output_file, engine="pyarrow", compression="snappy")

if __name__ == "__main__":
    input_file = 'title.akas.tsv'
    output_file = 'parquet/title.akas.parquet'

    clean_dataset(input_file, output_file)
