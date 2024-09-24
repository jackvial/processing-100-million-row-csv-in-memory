from memory_profiler import profile
import pandas as pd

@profile
def main():
    dtype_dict = {
        'chem_name': 'category',  # Use category type for strings to save memory
        'amount': 'float32',      # Use float32 to reduce memory for float columns
        'shipper': 'category',    # Use category for shipper strings
        'shipped_at': 'uint32'    # Use uint32 for smaller integers
    }
    df = pd.read_csv("outputs/chemicals_shipped_100000000.csv", dtype=dtype_dict)
    print(df.head())

if __name__ == "__main__":
    main()
    