import pl from 'nodejs-polars';
import fs from 'fs';
import {
    getMemoryStats,
    MemoryStatsRow,
    writeStatsToCsv
} from '../utils';

async function main() {
    const startTime = Date.now();
    const memoryStats: MemoryStatsRow[] = [];
    const nRows = 10_000_000;

    memoryStats.push(getMemoryStats(0));
    const df = pl.readCSV('outputs/test_10000000_rows.csv');
    memoryStats.push(getMemoryStats(nRows));
    console.log(df.head());
    
    // Group by 'color' and sum 'price'
    const out = df.groupBy('color').agg({
        price: ['sum']
    });

    // Write memory statistics to a CSV file
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/polars_${nRows}.csv`);
}

main();