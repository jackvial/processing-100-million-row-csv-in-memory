import polars as pl
from memory_profiler import profile

@profile
def main():
    schema = {
        "chem_name": pl.Categorical,
        "amount": pl.Float32,
        "shipper": pl.Categorical,
        "shipped_at": pl.UInt32,
    }

    dataset = pl.read_csv("outputs/chemicals_shipped_100000000.csv", dtypes=schema)
    print(dataset.head(5))

if __name__ == "__main__":
    main()
