import fs from 'fs';
import csvParser from 'csv-parser';
import { getMemoryStats } from '../utils';
import { StringColumnDict } from '../types';
import { finalizeDataFrame } from '../utils';

async function main(): Promise<void> {
    const nRows = 10_000_000;

    getMemoryStats(nRows);

    const colorArray: string[] = [];
    const priceArray: number[] = [];
    const skuArray: number[] = [];
    const isAvailableArray: boolean[] = [];

    getMemoryStats(nRows);

    let rowIndex = 0;
    const filePath = './outputs/test_10000000_rows.csv';
    const startReadFileTime = new Date().getTime();

    const readStream = fs.createReadStream(filePath);
    
    return new Promise<void>((resolve, reject) => {
        readStream
            .pipe(csvParser())
            .on('data', (row: any) => {
                if (rowIndex < nRows) {
                    // Parse the row data and push it into the respective arrays
                    skuArray.push(parseInt(row.sku));
                    priceArray.push(parseFloat(row.price));
                    isAvailableArray.push(row.isAvailable === 'true');
                    colorArray.push(row.color.replace(/"/g, ''));

                    // Log memory usage every 1,000,000 rows
                    if (rowIndex % 1_000_000 === 0) {
                        getMemoryStats(rowIndex);
                    }

                    rowIndex++;
                }
            })
            .on('end', () => {
                try {
                    const endReadFileTime = new Date().getTime();
                    console.log(`Time to read file: ${endReadFileTime - startReadFileTime} ms`);

                    getMemoryStats(nRows);

                    // Finalize the DataFrame after reading all rows
                    // const df = finalizeDataFrame({ skuArray, priceArray, isAvailableArray, colorArray });
                    // getMemoryStats({
                    //     nRows,
                    //     df
                    // });

                    // Group by color and sum the price
                    // console.time('Groupby color and Sum price');
                    // const groupedByColor = df.groupby('color');
                    // const summedFloatsByColor = df.sum(groupedByColor, 'price');
                    // console.log('Sum of price by color groups:', summedFloatsByColor);

                    resolve();
                } catch (err) {
                    reject(err);
                }
            })
            .on('error', (err) => {
                // Handle errors during file reading
                reject(err);
            });
    });
}

main();
