import os
import sqlite3
import pandas as pd
from slugify import slugify

def rename_duplicate_columns(df):
    """
    Rename duplicate columns in a DataFrame by appending a suffix to the column name.
    """
    cols = pd.Series(df.columns)
    
    # Examples: VEHICLE_TYPE', 'Vehicle_type', 'INFRASTRUCTURE_TYPE', 'Infrastructure_type', 'TIME_PERIOD', 'Time_period'
    # ALL_CAPS columns should be renamed with _CODE suffix, while lowercase columns should be kept as is

    for col in df.columns:
        if col.isupper():
            # If the column is all caps, keep it as is
            continue
        else:
            # If the column is not all caps and an all caps column exists, rename it with a suffix
            if col.upper() in cols.values:
                # Find the index of the first occurrence of the column
                idx = cols[cols == col].index[0]
                # Rename the column with a suffix
                new_col = f"{col}_CODE"
                df.rename(columns={col.upper(): new_col}, inplace=True)
                # Update the Series to reflect the new column name
                cols[idx] = new_col


def csv_to_db(csv_file, db_file):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create a table with the same name as the CSV file (without extension)
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    # Slugify the table name to ensure it's a valid SQLite identifier
    table_name = slugify(table_name, separator='_')
    print(f"Creating table: {table_name}")

    df.columns = [slugify(col, separator='_', lowercase=False) for col in df.columns]
    # Rename duplicate columns
    rename_duplicate_columns(df)
    df.columns = [slugify(col, separator='_') for col in df.columns]
    # Print the columns to be created
    print(f"Columns: {df.columns.tolist()}")

    # If all values in a column are NaN, drop that column
    df.dropna(axis=1, how='all', inplace=True)
    
    # Create the table with the appropriate columns
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Add unique values for each column to unique_values table
    # Create if it doesn't exist
    cursor.execute(f"CREATE TABLE IF NOT EXISTS unique_values (table_name TEXT, column_name TEXT, unique_value TEXT)")
    for col in df.columns:
        if col == "obs_value":
            continue
        # Get unique values for the column
        unique_values = df[col].dropna().unique()
        for value in unique_values:
            # Insert the unique value into the unique_values table
            cursor.execute(f"INSERT INTO unique_values (table_name, column_name, unique_value) VALUES (?, ?, ?)", (table_name, col, str(value)))

    # Commit and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Convert CSV to SQLite database.")
    parser.add_argument('--csv_dir', type=str, required=True, help='Directory containing CSV files.')
    parser.add_argument('--db_file', type=str, required=True, help='Path to the SQLite database file.')
    args = parser.parse_args()

    # Get a list of all CSV files in the specified directory
    csv_files = [os.path.join(args.csv_dir, f) for f in os.listdir(args.csv_dir) if f.endswith('.csv')]
    # Convert each CSV file to a table in the SQLite database
    for csv_file in csv_files:
        print(f"Converting {csv_file} to database...")
        csv_to_db(csv_file, args.db_file)
        print(f"Converted {csv_file} to database.")

    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(args.db_file)
    cursor = conn.cursor()

    # Create an index on the unique_values table for faster lookups
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_unique_values ON unique_values (table_name, column_name, unique_value)")

    # Commit and close the connection
    conn.commit()
    conn.close()