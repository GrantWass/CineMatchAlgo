import pandas as pd

data = pd.read_parquet("parquet/combined_imdb_data.parquet", engine="pyarrow")

frozen_iii_row = data[data['primaryTitle'].str.strip() == 'Frozen III']

if not frozen_iii_row.empty:
    print(frozen_iii_row)
else:
    print("'Frozen III' does not exist in the dataset.")

data = pd.read_csv("MvpDataset.csv")

frozen_iii_row = data[data['primaryTitle'].str.strip() == 'Frozen III']

if not frozen_iii_row.empty:
    print(frozen_iii_row)
else:
    print("'Frozen III' does not exist in the dataset.")

