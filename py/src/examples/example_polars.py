import polars as pl
from memory_profiler import profile

@profile
def main():
    df = pl.read_csv("outputs/chemicals_shipped_100000000.csv")
    df = df.group_by("color")
    df = df.agg(pl.col("price").sum())

    print(df.head())

if __name__ == "__main__":
    main()
