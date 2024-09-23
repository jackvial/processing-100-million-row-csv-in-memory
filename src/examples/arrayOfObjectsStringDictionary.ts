import {
    getMemoryStats,
    MemoryStatsRow,
    writeStatsToCsv
} from "../utils";
import csvParser from 'csv-parser';
import fs from 'fs';

export async function main() {
    const startTime = Date.now();
    const memoryStats: MemoryStatsRow[] = [];
    const nRows = 100_000_000;

    // Create maps for chemicals and shippers
    const chemMap: { [key: number]: string } = {};
    const chemStringToIndex: { [key: string]: number } = {};
    const shipperMap: { [key: number]: string } = {};
    const shipperStringToIndex: { [key: string]: number } = {};

    let chemIndex = 0;
    let shipperIndex = 0;

    const rows: any[] = [];
    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    await new Promise<void>((resolve, reject) => readStream.pipe(csvParser()).on('data', (row: any) => {
        if (!(row.chem_name in chemStringToIndex)) {
            chemStringToIndex[row.chem_name] = chemIndex;
            chemMap[chemIndex] = row.chem_name;
            chemIndex++;
        }

        if (!(row.shipper in shipperStringToIndex)) {
            shipperStringToIndex[row.shipper] = shipperIndex;
            shipperMap[shipperIndex] = row.shipper;
            shipperIndex++;
        }

        const chemIndexMapped = chemStringToIndex[row.chem_name];
        const shipperIndexMapped = shipperStringToIndex[row.shipper];

        rows.push({
            chemIndex: chemIndexMapped,
            amount: parseFloat(row.amount),
            shipperIndex: shipperIndexMapped,
            shippedAt: parseInt(row.shipped_at, 10)
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
