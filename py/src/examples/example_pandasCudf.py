import cudf.pandas
cudf.pandas.install()
import pandas as pd
import time

def main():
    start_time = time.time()
    dtype_dict = {
        'chem_name': 'category',  # Use category type for strings to save memory
        'amount': 'float32',      # Use float32 to reduce memory for float columns
        'shipper': 'category',    # Use category for shipper strings
        'shipped_at': 'uint32'    # Use uint32 for smaller integers
    }
    df = pd.read_csv("outputs/chemicals_shipped_100000000.csv", dtype=dtype_dict)
    df_grouped = df.groupby(['shipped_at', 'chem_name']).agg({'amount': 'sum'}).reset_index()
    df_grouped = df_grouped.sort_values(by='shipped_at', ascending=False)
    print(df_grouped.head())

    print("Time taken: ", time.time() - start_time)



if __name__ == "__main__":
    main()