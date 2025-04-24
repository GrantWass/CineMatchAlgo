import pandas as pd
import json

def load_name_mapping(names_parquet):
    """Load the mapping of nconst to primaryName."""
    names_df = pd.read_parquet(names_parquet)
    return dict(zip(names_df['nconst'], names_df['primaryName']))

def replace_principals_with_names(crew_parquet, names_parquet, output_parquet):
    # Load data
    crew_df = pd.read_parquet(crew_parquet)
    name_mapping = load_name_mapping(names_parquet)

    def map_principals(principals):
        try:
            if isinstance(principals, str):
                principals_list = json.loads(principals.replace("'", '"'))  # Ensure valid JSON format
            else:
                principals_list = principals  # Already a list (just in case)

            # Extract names instead of nconst
            names = [name_mapping.get(person['nconst'], person['nconst']) for person in principals_list if 'nconst' in person]
            return names
        except Exception as e:
            print(f"Error processing principals: {e}")
            return []

    # Apply mapping to principals column
    crew_df['principals'] = crew_df['principals'].apply(map_principals)

    # Save the result
    crew_df.to_parquet(output_parquet, index=False)
    print(f"Processed principals data saved to {output_parquet}")

if __name__ == "__main__":
    replace_principals_with_names("../parquet/title.principals.parquet", "../parquet/name.basics.parquet", "../parquet/principal_names.parquet")
