import pandas as pd

def clean_column(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce') 
    return df

def clean_dataset(input_file, output_file):    
    data = pd.read_csv(input_file, sep="\t", low_memory=False, na_values=["\\N"])

    data = data.dropna(subset=['primaryTitle'])
    valid_title_types = ['movie']

    # This has like two million rows, maybe a bit much
    # valid_title_types = ['short', 'movie', 'tvMovie', 'video']

    data = data[data['titleType'].isin(valid_title_types)]
    
    data = clean_column(data, "runtimeMinutes")

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
    input_file = 'title.basics.tsv'
    output_file = 'parquet/title.basics.parquet'
    
    clean_dataset(input_file, output_file)
