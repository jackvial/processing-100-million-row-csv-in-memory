import {
    prettyPrintMemoryUsage,
} from "../utils"
import csvParser from 'csv-parser';
import fs from 'fs';

export async function main() {
    const nRows = 10_000_000;
    prettyPrintMemoryUsage({ nRows });

    console.time('Create Data');
    const rows: any[] = [];
    const filePath = 'outputs/test_10000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    await new Promise<void>((resolve, reject) => readStream.pipe(csvParser()).on('data', (row: any) => {
        rows.push(row);

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
    const priceByColor: { [key: string]: number } = {};
    for (let i = 0; i < nRows; i++) {
        const row = rows[i];
        const price = parseFloat(row.price);
        const color = row.color;
        if (!priceByColor[color]) {
            priceByColor[color] = 0;
        }
        priceByColor[color] += price;
    }

    prettyPrintMemoryUsage({ nRows });

    console.log('---------------------------------');
    console.timeEnd('Group Price By Color');
    console.log(priceByColor);
}

main();