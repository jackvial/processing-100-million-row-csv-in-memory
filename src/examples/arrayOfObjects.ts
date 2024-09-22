import {
    getMemoryStats,
    MemoryStatsRow,
    writeStatsToCsv
} from "../utils"
import csvParser from 'csv-parser';
import fs from 'fs';

export async function main() {
    const startTime = Date.now();
    const memoryStats: MemoryStatsRow[] = [];
    const nRows = 10_000_000;
    const rows: any[] = [];
    const filePath = 'outputs/test_10000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    await new Promise<void>((resolve, reject) => readStream.pipe(csvParser()).on('data', (row: any) => {
        rows.push(row);

        if (rowIndex % 1_000_000 === 0) {
            memoryStats.push(getMemoryStats(rowIndex));
        }

        rowIndex++;
    }).on('end', () => {
        resolve();
    }).on('error', (error) => {
        reject(error);
    }));

    memoryStats.push(getMemoryStats(nRows));
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/arrayOfObjects_${rows.length}.csv`);
}

main();