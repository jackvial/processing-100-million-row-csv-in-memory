import fs from 'fs';
import readline from 'readline';
import { getMemoryStats, allocateBuffers, populateBuffersFromRow } from '../utils';
import {StringColumnDict} from '../types';
import {finalizeDataFrame} from '../utils';

async function main(): Promise<void> {
    const nRows = 10_000_000;

    getMemoryStats(nRows);

    // Define the schema and allocate buffers
    const columns = allocateBuffers(nRows, [
        { name: 'SKU', dataType: 'int32' },
        { name: 'price', dataType: 'float32' },
        { name: 'isAvailable', dataType: 'bool' },
        { name: 'color', dataType: 'string' }
    ]);

    getMemoryStats(nRows);

    const stringColumnDicts: { [key: string]: StringColumnDict } = {
        color: {
            valueToIndex: { red: 0, green: 1, blue: 2, purple: 3 },
            strings: ['red', 'green', 'blue', 'purple'],
        },
    };

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

                // Map rowData to an object matching column names
                const parsedRow = {
                    SKU: rowData[0],
                    price: rowData[1],
                    isAvailable: rowData[2],
                    // color: rowData[3]
                    color: rowData[3].replace(/"/g, '')  // Strip quotes from the color field
                };

                // Populate the buffers with the parsed row data
                populateBuffersFromRow(rowIndex, parsedRow, columns, stringColumnDicts);

                if (rowIndex % 1_000_000 === 0) {
                    getMemoryStats(rowIndex);
                }

                rowIndex++;
            }
        });

        rl.on('close', () => {
            try {
                const endReadFileTime = new Date().getTime();
                console.log(`Time to read file: ${endReadFileTime - startReadFileTime} ms`);

                getMemoryStats(nRows);

                // Finalize the DataFrame after reading all rows
                const df = finalizeDataFrame(columns);
                getMemoryStats(nRows, df);

                // Group by color and sum the price
                console.time('Groupby color and Sum price');
                const groupedByColor = df.groupby('color');
                const summedFloatsByColor = df.sum(groupedByColor, 'price');
                console.log('Sum of price by color groups:', summedFloatsByColor);

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