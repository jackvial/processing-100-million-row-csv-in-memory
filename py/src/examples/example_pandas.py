import pandas as pd
from memory_profiler import profile

@profile
def main():
    # Read the CSV file into a pandas DataFrame, selecting specific columns
    df = pd.read_csv("outputs/test_100000000_rows.csv", usecols=["color", "price"])

    # Group by 'color' and compute the sum of 'price'
    grouped_df = df.groupby("color")["price"].sum().reset_index()

    # Print the resulting DataFrame
    print(grouped_df)

if __name__ == "__main__":
    main()