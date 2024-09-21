import pyarrow.dataset as ds
import pyarrow.compute as pc
from memory_profiler import profile

@profile
def main():
    # Create a Dataset from the CSV file
    dataset = ds.dataset("outputs/test_100000000_rows.csv", format="csv")

    # Define the fields to select (optional)
    fields = ["color", "price"]

    # Scan the Dataset and convert it to a Table
    table = dataset.to_table(columns=fields)

    # Group by 'color' and compute the sum of 'price'
    grouped_table = table.group_by("color").aggregate([("price", "sum")])

    # Convert the result to a pandas DataFrame
    df = grouped_table.to_pandas()
    print(df)

if __name__ == "__main__":
    main()