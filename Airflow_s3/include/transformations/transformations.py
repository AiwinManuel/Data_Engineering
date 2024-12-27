import io
import pandas as pd

def transform_data(df):
    # Fill column-level null values with column median
    df.fillna(df.median(numeric_only=True), inplace=True)
    print("DataFrame after replacing null values with column medians:")
    print(df)

    # Remove Duplicates from the DataFrame
    duplicates = df.duplicated()
    print("Number of duplicate rows:", duplicates.sum())
    df.drop_duplicates(inplace=True)

    # Convert the DataFrame to a CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()
