import pandas as pd


# NOT BEING USED CURRENTLY

def convertToParquetInChunks(tsv_file, parquet_file, chunksize=500_000):
    print(f"Converting {tsv_file} to {parquet_file}")
    
    parquet_chunks = []
    for chunk in pd.read_csv(tsv_file, sep="\t", low_memory=True, na_values=["\\N"], chunksize=chunksize):
        parquet_chunks.append(chunk)

    df = pd.concat(parquet_chunks)
    df.to_parquet(parquet_file, engine="pyarrow", compression="snappy")
    

tsv_files = [
    "title.basics.tsv",
    "title.akas.tsv",
    "title.crew.tsv",
    "title.principals.tsv",
    "title.ratings.tsv",
]

for tsv in tsv_files:
    parquet = tsv.replace(".tsv", ".parquet")
    convertToParquetInChunks(tsv, parquet)

print("All files converted to Parquet! 🚀")
