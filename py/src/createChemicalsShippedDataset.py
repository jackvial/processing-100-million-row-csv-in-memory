import pandas as pd
import random
from datetime import datetime, timedelta
from tqdm import tqdm  # Import tqdm for the progress bar

def main():
    # Selected 8 chemicals from different categories
    chemicals = [
        'Sulfuric Acid', 'Hydrochloric Acid', 'Sodium Hydroxide', 'Ammonium Nitrate',
        'Ethylene Glycol', 'Methanol', 'Acetone', 'Chlorine'
    ]
    
    shippers = ['Clean Docks', 'Clean Planet', 'Eco Shipping', 'Green Chemicals Ltd.']

    start_date = datetime(2000, 1, 1)  # Starting date
    rows = []
    
    # Generate 1000 items per day, adding tqdm for progress tracking
    for day in tqdm(range(0, 100000), desc="Generating daily shipments"):
        current_date = start_date + timedelta(days=day)
        timestamp = int(current_date.timestamp())

        for _ in range(1000):  # Generate 1000 rows per day
            amount = random.uniform(0.01, 100)
            chem_name = random.choice(chemicals)
            shipper = random.choice(shippers)

            rows.append({
                'chem_name': chem_name,
                'amount': amount,
                'shipper': shipper,
                'shipped_at': timestamp
            })
    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    print(df.head())  # Display the first few rows

    # shuffle the rows
    df = df.sample(frac=1).reset_index(drop=True)

    # Save to CSV
    df.to_csv(f"/home/jack/code/df.ts/outputs/chemicals_shipped_{100_000_000}.csv", index=False)
    
if __name__ == '__main__':
    main()
