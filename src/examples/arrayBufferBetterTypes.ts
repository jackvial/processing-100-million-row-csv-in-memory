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

    // Create a single ArrayBuffer large enough to hold all data
    const buffer = new ArrayBuffer(nRows * (1 + 4 + 1 + 4)); // chem (1 byte), amount (4 bytes), shipper (1 byte), shippedAt (4 bytes)

    // Create TypedArray views over the ArrayBuffer
    const chemArray = new Uint8Array(buffer, 0, nRows);       // 1 byte per entry (Uint8Array for 256 unique chemicals)
    const amountArray = new Float32Array(buffer, nRows, nRows);  // 4 bytes per entry (Float32Array for the amount)
    const shipperArray = new Uint8Array(buffer, nRows * 3, nRows);  // 1 byte per entry (Uint8Array for 256 unique shippers)
    const shippedAtArray = new Uint32Array(buffer, nRows * 4, nRows);  // 4 bytes per entry (Uint32 for Unix timestamp)

    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);
    let rowIndex = 0;

    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser()).on('data', (row: any) => {
            // Build chemical map dynamically
            if (!(row.chem_name in reverseChemMap)) {
                if (chemIndex > 255) {
                    throw new Error("Too many unique chemicals to fit in Uint8Array (max 256)");
                }
                reverseChemMap[row.chem_name] = chemIndex;
                chemMap[chemIndex] = row.chem_name;
                chemIndex++;
            }

            // Build shipper map dynamically
            if (!(row.shipper in reverseShipperMap)) {
                if (shipperIndex > 255) {
                    throw new Error("Too many unique shippers to fit in Uint8Array (max 256)");
                }
                reverseShipperMap[row.shipper] = shipperIndex;
                shipperMap[shipperIndex] = row.shipper;
                shipperIndex++;
            }

            const chemIndexMapped = reverseChemMap[row.chem_name];
            const shipperIndexMapped = reverseShipperMap[row.shipper];

            chemArray[rowIndex] = chemIndexMapped;
            amountArray[rowIndex] = parseFloat(row.amount); // Direct Float16 representation
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
    }, `stats/arrayBufferBetterTypes_${nRows}.csv`);
}

main();
