import fs from 'fs';
import csvParser from 'csv-parser';
import {
    getMemoryStats,
    MemoryStatsRow,
    writeStatsToCsv
} from "../utils";

export async function main() {
    const startTime = Date.now();
    const memoryStats: MemoryStatsRow[] = [];
    const nRows = 100_000_000;
    memoryStats.push(getMemoryStats(0));

    // Dynamic dictionaries to map chemicals and shippers
    const chemMap: { [key: number]: string } = {};
    const reverseChemMap: { [key: string]: number } = {};
    let chemIndex = 0;

    const shipperMap: { [key: number]: string } = {};
    const reverseShipperMap: { [key: string]: number } = {};
    let shipperIndex = 0;

    console.time('Create Buffer');

    // Create a single ArrayBuffer large enough to hold all data
    const buffer = new ArrayBuffer(nRows * (4 + 8 + 4 + 4)); // chem (4 bytes), amount (8 bytes), shipper (4 bytes), shippedAt (4 bytes)

    // Create TypedArray views over the ArrayBuffer
    const chemArray = new Uint32Array(buffer, 0, nRows);       // 4 bytes per entry
    const amountArray = new Float64Array(buffer, nRows * 4, nRows);  // 8 bytes per entry
    const shipperArray = new Uint32Array(buffer, nRows * 12, nRows);  // 4 bytes per entry
    const shippedAtArray = new Uint32Array(buffer, nRows * 16, nRows);  // 4 bytes per entry (store Unix timestamp as a number)

    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);
    let rowIndex = 0;

    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser()).on('data', (row: any) => {
            // Build chemical map dynamically
            if (!(row.chem_name in reverseChemMap)) {
                reverseChemMap[row.chem_name] = chemIndex;
                chemMap[chemIndex] = row.chem_name;
                chemIndex++;
            }

            // Build shipper map dynamically
            if (!(row.shipper in reverseShipperMap)) {
                reverseShipperMap[row.shipper] = shipperIndex;
                shipperMap[shipperIndex] = row.shipper;
                shipperIndex++;
            }

            const chemIndexMapped = reverseChemMap[row.chem_name];
            const shipperIndexMapped = reverseShipperMap[row.shipper];

            chemArray[rowIndex] = chemIndexMapped;
            amountArray[rowIndex] = parseFloat(row.amount);
            shipperArray[rowIndex] = shipperIndexMapped;
            shippedAtArray[rowIndex] = parseInt(row.shipped_at, 10);  // Unix timestamp fits into 32-bit integer

            rowIndex++;

            if (rowIndex % 1_000_000 === 0) {
                memoryStats.push(getMemoryStats(rowIndex));
            }
        })
        .on('end', () => { 
            resolve();
        })
        .on('error', (error) => {
            reject(error);
        });
    });

    memoryStats.push(getMemoryStats(nRows));
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/arrayBuffer_${nRows}.csv`);
}

main();
