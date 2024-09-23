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

    // Adjust the buffer size to include 1 extra byte per row for null tracking
    const buffer = new ArrayBuffer(nRows * (4 + 8 + 4 + 4 + 1)); // chem (4 bytes), amount (8 bytes), shipper (4 bytes), shippedAt (4 bytes), null tracking (1 byte)

    // Create TypedArray views over the ArrayBuffer
    const chemArray = new Uint32Array(buffer, 0, nRows);       // 4 bytes per entry
    const amountArray = new Float64Array(buffer, nRows * 4, nRows);  // 8 bytes per entry
    const shipperArray = new Uint32Array(buffer, nRows * 12, nRows);  // 4 bytes per entry
    const shippedAtArray = new Uint32Array(buffer, nRows * 16, nRows);  // 4 bytes per entry (store Unix timestamp as a number)
    const nullTrackingArray = new Uint8Array(buffer, nRows * 20, nRows);  // 1 byte per row for null tracking (starts after the shippedAtArray)

    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);
    let rowIndex = 0;

    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser()).on('data', (row: any) => {
            let nullByte = 0;  // Use a byte to track nulls for the current row

            // Check and store null status for each column in a bit
            if (row.chem_name === null || row.chem_name === '') {
                nullByte |= (1 << 0);  // Set the 1st bit for chem_name
            } else {
                if (!(row.chem_name in reverseChemMap)) {
                    reverseChemMap[row.chem_name] = chemIndex;
                    chemMap[chemIndex] = row.chem_name;
                    chemIndex++;
                }
                chemArray[rowIndex] = reverseChemMap[row.chem_name];
            }

            if (row.amount === null || row.amount === '') {
                nullByte |= (1 << 1);  // Set the 2nd bit for amount
            } else {
                amountArray[rowIndex] = parseFloat(row.amount);
            }

            if (row.shipper === null || row.shipper === '') {
                nullByte |= (1 << 2);  // Set the 3rd bit for shipper
            } else {
                if (!(row.shipper in reverseShipperMap)) {
                    reverseShipperMap[row.shipper] = shipperIndex;
                    shipperMap[shipperIndex] = row.shipper;
                    shipperIndex++;
                }
                shipperArray[rowIndex] = reverseShipperMap[row.shipper];
            }

            if (row.shipped_at === null || row.shipped_at === '') {
                nullByte |= (1 << 3);  // Set the 4th bit for shipped_at
            } else {
                shippedAtArray[rowIndex] = parseInt(row.shipped_at, 10);
            }

            // Store the null byte for this row in the null tracking array
            nullTrackingArray[rowIndex] = nullByte;

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