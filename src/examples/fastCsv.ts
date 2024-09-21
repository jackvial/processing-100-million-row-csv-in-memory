import fs from 'fs';
import { parse } from 'fast-csv';
import { prettyPrintMemoryUsage } from "../utils";

function main() {
    const rows: any[] = [];
    const filePath = 'outputs/test_100000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    const csvStream = parse({ headers: true })  // `headers: true` if your CSV has a header row
        .on('data', (row: any) => {
            // rows.push(row);

            if (rowIndex % 1_000_000 === 0) {
                prettyPrintMemoryUsage({
                    nRows: rowIndex,
                });
            }

            rowIndex++;
        })
        .on('end', () => {
            console.log(`Total rows processed: ${rowIndex}`);
            console.log(`Rows stored in memory: ${rows.length}`);
        })
        .on('error', (err: Error) => {
            console.error('Error reading CSV:', err);
        });

    readStream.pipe(csvStream);
}

main();
