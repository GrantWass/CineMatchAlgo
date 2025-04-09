import pandas as pd

if __name__ == "__main__":
    final_df = pd.read_parquet("./parquet/final_combined_dataset.parquet")
    print(final_df.head(2)) 

    first_row = final_df.iloc[0]  
    print("Columns in first row:")
    for col, val in first_row.items():
        print(f"{col}: {val}")

    print("\nNull counts per column:")
    print(final_df.isnull().sum())

    # diff_count = (final_df["primaryTitle"] != final_df["originalTitle"]).sum()
    # print(f"Rows where primaryTitle != originalTitle: {diff_count}")

    print(f"Total rows: {len(final_df)}")

    columns_to_drop = ["all_crew", "principals", "Title", "principals_x", "writers", "directors", "endYear", "titleType"]
    cleaned_df = final_df.drop(columns=columns_to_drop)

    # Save to CSV
    cleaned_df.to_csv("MvpDataset.csv", index=False)
    print("Saved cleaned dataset as 'MvpDataset.csv'")

