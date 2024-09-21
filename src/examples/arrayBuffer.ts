import fs from 'fs';
import csvParser from 'csv-parser';
import {
    prettyPrintMemoryUsage,
} from "../utils"

export async function main() {
    const nRows = 100_000_000;
    prettyPrintMemoryUsage({
        nRows
    });

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
        })
        .on('end', () => { 
            resolve();
        })
        .on('error', (error) => {
            reject(error);
        });
    });


    prettyPrintMemoryUsage({
        nRows
    });

    console.log('---------------------------------');
    console.time('Group Price By Color');

    // Group price by color and sum each group using a for loop, no object copy
    const priceByColor: { [key: string]: number } = {
        red: 0,
        green: 0,
        blue: 0,
        purple: 0
    };

    for (let i = 0; i < nRows; i++) {
        const price = priceArray[i];
        const colorIndex = colorArray[i];
        const color = colorMap[colorIndex];
        priceByColor[color] += price;
    }

    prettyPrintMemoryUsage({
        nRows
    });

    console.log('---------------------------------');
    console.timeEnd('Group Price By Color');

    console.log(priceByColor);
}

main();