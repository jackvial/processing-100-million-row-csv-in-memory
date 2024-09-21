import fs from 'fs';
import csvParser from 'csv-parser';

export function main () {
    const rows: any[] = [];
    const filePath = 'outputs/test_100000000_rows.csv';
    const readStream = fs.createReadStream(filePath);
    readStream.pipe(csvParser()).on('data', (row: any) => { rows.push(row); });

    const priceByColor: { [key: string]: number } = {};
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const price = row.price;
        const color = row.color;
        if (!priceByColor[color]) {
            priceByColor[color] = 0;
        }
        priceByColor[color] += price;
    }

    console.log(priceByColor);
}

main();