import {
    prettyPrintMemoryUsage,
} from "../utils"
import csvParser from 'csv-parser';
import fs from 'fs';

export async function main() {
    const nRows = 100_000_000;
    prettyPrintMemoryUsage({ nRows });

    // Color map to store smaller integers instead of full color names
    const colorMap = {
        0: 'red',
        1: 'green',
        2: 'blue',
        3: 'purple'
    };

    // A reverse lookup for color string to integer mapping
    const colorStringToIndex = {
        'red': 0,
        'green': 1,
        'blue': 2,
        'purple': 3
    };

    console.time('Create Data');
    const rows: any[] = [];
    const filePath = 'outputs/test_100000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    await new Promise<void>((resolve, reject) => readStream.pipe(csvParser()).on('data', (row: any) => {
        // Convert color to an integer index and store it
        const colorIndex = colorStringToIndex[row.color];
        rows.push({
            price: parseFloat(row.price),
            colorIndex // Store the index instead of the color name
        });

        if (rowIndex % 1_000_000 === 0) {
            prettyPrintMemoryUsage({
                nRows: rowIndex,
            });
        }

        rowIndex++;
    }).on('end', () => {
        resolve();
    }).on('error', (error) => {
        reject(error);
    }));

    prettyPrintMemoryUsage({ nRows });
    console.log('---------------------------------');
    console.timeEnd('Create Data');

    console.time('Group Price By Color');
    const priceByColorIndex: { [key: number]: number } = {};

    // Group by color index and sum prices
    for (let i = 0; i < nRows; i++) {
        const row = rows[i];
        const price = row.price;
        const colorIndex = row.colorIndex;

        if (!priceByColorIndex[colorIndex]) {
            priceByColorIndex[colorIndex] = 0;
        }
        priceByColorIndex[colorIndex] += price;
    }

    prettyPrintMemoryUsage({ nRows });

    console.log('---------------------------------');
    console.timeEnd('Group Price By Color');

    // Convert color index back to actual color for display using a for loop
    const priceByColor: { [key: string]: number } = {};
    for (const colorIndex in priceByColorIndex) {
        if (priceByColorIndex.hasOwnProperty(colorIndex)) {
            const colorName = colorMap[parseInt(colorIndex)];
            priceByColor[colorName] = priceByColorIndex[colorIndex];
        }
    }

    console.log(priceByColor);
}

main();
