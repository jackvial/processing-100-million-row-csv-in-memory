import pandas as pd
import random
import time
from datetime import datetime, timedelta

def main():
    chemicals = ['Sulfuric Acid', 'Hydrochloric Acid', 'Nitric Acid', 'Phosphoric Acid']
    shippers = ['Clean Docks', 'Clean Planet']

    start_date = datetime(2000, 1, 1)  # Starting date
    rows = []
    
    # We want to generate 1000 items per day
    for day in range(0, 100000):
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
    
    # Convert to DataFrame (optional, for handling large data)
    df = pd.DataFrame(rows)
    print(df.head())  # Display the first few rows
    
if __name__ == '__main__':
    main()
