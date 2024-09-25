import pyarrow as pa
import pandas as pd
import random
from datetime import datetime, timedelta
from tqdm import tqdm
import os

def main():
    # Selected 8 chemicals from different categories
    chemicals = [
        'Sulfuric Acid', 'Hydrochloric Acid', 'Sodium Hydroxide', 'Ammonium Nitrate',
        'Ethylene Glycol', 'Methanol', 'Acetone', 'Chlorine'
    ]
    
    shippers = ['Clean Docks', 'Clean Planet', 'Eco Shipping', 'Green Chemicals Ltd.']

    start_date = datetime(1996, 1, 1)
    
    # Set up schema for the Arrow Table
    schema = pa.schema([
        ('chem_name', pa.string()),
        ('amount', pa.float64()),
        ('shipper', pa.string()),
        ('shipped_at', pa.int64()),
    ])

    # Create outputs directory if it doesn't exist
    os.makedirs("outputs", exist_ok=True)
    
    batch_size = 10000  # Define a batch size to process
    file_path = f"outputs/chemicals_shipped_{100_000_000}.csv"
    
    # Flag to indicate if the header needs to be written
    header_written = False
    
    # Process 10k days of data, roughly ~27 years
    for day in tqdm(range(0, 10000), desc="Generating daily shipments"):
        current_date = start_date + timedelta(days=day)
        timestamp = int(current_date.timestamp())

        # Collect batch records
        records = [
            (
                random.choice(chemicals),
                random.uniform(0.01, 100),
                random.choice(shippers),
                timestamp
            ) for _ in range(batch_size)
        ]
        
        # Convert records to Arrow table
        batch = pa.Table.from_pydict({
            'chem_name': [r[0] for r in records],
            'amount': [r[1] for r in records],
            'shipper': [r[2] for r in records],
            'shipped_at': [r[3] for r in records],
        }, schema=schema)

        # Convert Arrow Table to Pandas DataFrame for CSV writing
        df = batch.to_pandas()

        # Write to CSV using Pandas
        mode = 'w' if not header_written else 'a'  # Write mode: 'w' for write (first time), 'a' for append
        df.to_csv(file_path, mode=mode, index=False, header=not header_written)
        
        # After the first write, header is written
        header_written = True
    
    print(f"Data written to {file_path}")

if __name__ == '__main__':
    main()
