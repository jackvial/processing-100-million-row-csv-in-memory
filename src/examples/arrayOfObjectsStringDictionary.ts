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

    const rows: any[] = [];
    const filePath = 'outputs/test_10000000_rows.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    await new Promise<void>((resolve, reject) => readStream.pipe(csvParser()).on('data', (row: any) => {
        const colorIndex = colorStringToIndex[row.color];
        rows.push({
            price: parseFloat(row.price),
            colorIndex
        });

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
    }, `stats/arrayOfObjectsStringDictionary_${rows.length}.csv`);
}

main();
