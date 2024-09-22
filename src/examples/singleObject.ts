import {
    getMemoryStats,
} from "../utils"
import csvParser from 'csv-parser';
import fs from 'fs';

export async function main() {
    const nRows = 10_000_000;
    getMemoryStats(nRows);

    console.time('Process CSV and Group Price By Color');
    const priceByColor: { [key: string]: number } = {};
    const filePath = 'outputs/test_10000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser())
            .on('data', (row: any) => {
                const price = parseFloat(row.price);
                const color = row.color;

                if (!priceByColor[color]) {
                    priceByColor[color] = 0;
                }
                priceByColor[color] += price;

                if (rowIndex % 1_000_000 === 0) {
                    getMemoryStats(rowIndex);
                }

                rowIndex++;
            })
            .on('end', () => {
                resolve();
            })
            .on('error', (error) => {
                reject(error);
            });
    });

    getMemoryStats(nRows);
    console.log('---------------------------------');
    console.timeEnd('Process CSV and Group Price By Color');
    console.log(priceByColor);
}

main();