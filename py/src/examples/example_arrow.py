import pyarrow.dataset as ds
import pyarrow as pa
from memory_profiler import profile

schema = pa.schema([
    ('chem_name', pa.dictionary(index_type=pa.int32(), value_type=pa.string())),
    ('amount', pa.float32()),
    ('shipper', pa.dictionary(index_type=pa.int32(), value_type=pa.string())),
    ('shipped_at', pa.uint32()),
])

@profile
def main():
    dataset = ds.dataset("outputs/chemicals_shipped_100000000.csv", format="csv", schema=schema)
    table = dataset.to_table()
    print(table.slice(0, 5))

if __name__ == "__main__":
    main()