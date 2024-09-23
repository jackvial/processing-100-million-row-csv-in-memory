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
    const nRows = 100_000_000;

    memoryStats.push(getMemoryStats(0));
    import pl from 'nodejs-polars';
    const df = pl.readCSV('outputs/chemicals_shipped_100000000.csv');
    console.log(df.head());
    memoryStats.push(getMemoryStats(nRows));

    // Write memory statistics to a CSV file
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/polars_${nRows}.csv`);
}

main();