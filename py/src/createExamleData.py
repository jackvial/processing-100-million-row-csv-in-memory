import pandas as pd
import random

def main():
    df = pd.read_csv('/Users/jackvial/Downloads/EM_WASTE_LINE/EM_WASTE_LINE_26.csv')
    print(len(df.columns))
    print(df.head(50))

    # MANIFEST_TRACKING_NUMBER, WASTE_LINE_NUMBER, QUANTITY_HAZ_TONS, QUANTITY_NON_HAZ_TONS
    # 200769224CLE, 1, 0.0005, 0
    # 200769224CLE, 2, 0, 0.1610

    # Generate 100 million rows of data in the format above
    # should have 10 unique manifest tracking numbers
    manifest_tracking_number = ['100100000ABC', '200200000DEF', '300300000GHI', '400400000JKL', '500500000MNO', '600600000PQR', '700700000STU', '800800000VWX', '900900000YZA', '000000000BCD']
    rows = []
    for manifest in manifest_tracking_number:
        for i in range(1, 10000000):
            # generate 0.001 and 100 for QUANTITY_HAZ_TONS and QUANTITY_NON_HAZ_TONS
            
            if i % 10 == 0:
                qty_haz_tons = 0
                qty_non_haz_tons = random.uniform(0.001, 100)
            else:
                qty_haz_tons = random.uniform(0.001, 100)
                qty_non_haz_tons = 0

            rows.append({'MANIFEST_TRACKING_NUMBER': manifest, 'WASTE_LINE_NUMBER': i, 'QUANTITY_HAZ_TONS': qty_haz_tons, 'QUANTITY_NON_HAZ_TONS': qty_non_haz_tons})

    df = pd.DataFrame(rows)
    print(df.head(50))

    output_file = '/Users/jackvial/Code/ts/df.ts/outputs/FAKE_EM_WASTE_LINE_100000000.csv'
    df.to_csv(output_file, index=False)




if __name__ == '__main__':
    main()