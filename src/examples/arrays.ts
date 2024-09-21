import fs from 'fs';
import readline from 'readline';
import { prettyPrintMemoryUsage, allocateBuffers, populateBuffersFromRow } from '../utils';
import {StringColumnDict} from '../types';
import {finalizeDataFrame} from '../utils';

async function main(): Promise<void> {
    const nRows = 10_000_000;

    prettyPrintMemoryUsage({
        nRows,
    });

    const colorArray: string[] = [];
    const priceArray: number[] = [];
    const skuArray: number[] = [];
    const isAvailableArray: boolean[] = [];

    prettyPrintMemoryUsage({
        nRows,
    });

    let rowIndex = 0;
    const fileStream = fs.createReadStream("./outputs/test_10000000_rows.csv");

    // Use readline to read the CSV file line by line
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const startReadFileTime = new Date().getTime();

    let isFirstLine = false;

    return new Promise<void>((resolve, reject) => {
        rl.on('line', (line) => {
            if (!isFirstLine) {
                isFirstLine = true;
                return;
            }
            if (rowIndex < nRows) {
                // Split the line by comma (assuming a simple CSV format)
                const rowData = line.split(',');

                skuArray.push(parseInt(rowData[0]));
                priceArray.push(parseFloat(rowData[1]));
                isAvailableArray.push(rowData[2] === 'true');
                colorArray.push(rowData[3].replace(/"/g, ''));

                if (rowIndex % 1_000_000 === 0) {
                    prettyPrintMemoryUsage({
                        nRows: rowIndex,
                    });
                }

                rowIndex++;
            }
        });

        rl.on('close', () => {
            try {
                const endReadFileTime = new Date().getTime();
                console.log(`Time to read file: ${endReadFileTime - startReadFileTime} ms`);

                prettyPrintMemoryUsage({
                    nRows: nRows,
                });

                // Finalize the DataFrame after reading all rows
                // const df = finalizeDataFrame(columns);
                // prettyPrintMemoryUsage({
                //     nRows,
                //     df
                // });

                // // Group by color and sum the price
                // console.time('Groupby color and Sum price');
                // const groupedByColor = df.groupby('color');
                // const summedFloatsByColor = df.sum(groupedByColor, 'price');
                // console.log('Sum of price by color groups:', summedFloatsByColor);

                // Resolve the promise
                resolve();
            } catch (err) {
                // Reject the promise if any error occurs
                reject(err);
            }
        });

        rl.on('error', (err) => {
            // Handle errors in reading the file
            reject(err);
        });
    });
}

main();