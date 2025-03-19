import pandas as pd

def clean_column(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce') 
    return df

def clean_dataset(input_file, output_file):    
    data = pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"])
    data = data[['title', 'region', 'language', 'types', 'isOriginalTitle', 'titleId']]

    print(data.head())    
    print(data.dtypes)
    print(data.isnull().sum())
    print(data.columns)
    print(data.shape)

    # for col in data.select_dtypes(include=['object']).columns:
    #     print(f"\nColumn: {col}")
    #     print(data[col].unique())
    
    data.to_parquet(output_file, engine="pyarrow", compression="snappy")

if __name__ == "__main__":
    input_file = 'title.akas.tsv'
    output_file = 'parquet/title.akas.parquet'
    
    clean_dataset(input_file, output_file)
