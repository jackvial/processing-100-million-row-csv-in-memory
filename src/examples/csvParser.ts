import fs from 'fs';
import csvParser from 'csv-parser';
import {
    prettyPrintMemoryUsage
} from "../utils"

function main() {
    const rows: any[] = [];
    const filePath = 'outputs/test_100000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    readStream
    .pipe(csvParser())
    .on('data', (row: any) => {
        rows.push(row);

        if (rowIndex % 1_000_000 === 0) {
            prettyPrintMemoryUsage({
                nRows: rowIndex,
            });
        }

        rowIndex++;
    })
    .on('end', () => {
        console.log(rows.length);
    })
    .on('error', (err: Error) => {
        console.error('Error reading CSV:', err);
    });
    
}

main();