import fs from 'fs';
import csvParser from 'csv-parser';
import {
    getMemoryStats,
    MemoryStatsRow,
    writeStatsToCsv
} from "../utils"

export async function main() {
    const startTime = Date.now();
    const memoryStats: MemoryStatsRow[] = [];
    const nRows = 10_000_000;
    memoryStats.push(getMemoryStats(0));

    // Define constants for colors
    const colorMap = {
        0: 'red',
        1: 'green',
        2: 'blue',
        3: 'purple'
    };

    const reverseColorMap: { [key: string]: number } = {
        red: 0,
        green: 1,
        blue: 2,
        purple: 3
    };

    console.time('Create Buffer');

    // Create a single ArrayBuffer large enough to hold all data
    const buffer = new ArrayBuffer(nRows * (4 + 4 + 1 + 1)); // SKU (4 bytes), price (4 bytes), isAvailable (1 byte), color (1 byte)

    // Create TypedArray views over the ArrayBuffer
    const skuArray = new Uint32Array(buffer, 0, nRows);       // 4 bytes per entry
    const priceArray = new Float32Array(buffer, nRows * 4, nRows);  // 4 bytes per entry
    const isAvailableArray = new Uint8Array(buffer, nRows * 8, nRows);  // 1 byte per entry
    const colorArray = new Uint8Array(buffer, nRows * 9, nRows);  // 1 byte per entry (store color as a number)

    const filePath = 'outputs/test_10000000_rows.csv';
    const readStream = fs.createReadStream(filePath);
    let rowIndex = 0;

    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser()).on('data', (row: any) => {
            skuArray[rowIndex] = parseInt(row.sku);
            priceArray[rowIndex] = parseFloat(row.price);
            isAvailableArray[rowIndex] = row.isAvailable === 'true' ? 1 : 0;
            colorArray[rowIndex] = reverseColorMap[row.color];
            rowIndex++;

            if (rowIndex % 1_000_000 === 0) {
                memoryStats.push(getMemoryStats(rowIndex));
            }
        })
        .on('end', () => { 
            resolve();
        })
        .on('error', (error) => {
            reject(error);
        });
    });


    memoryStats.push(getMemoryStats(nRows));
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/arrayBuffer_${nRows}.csv`);
}

main();