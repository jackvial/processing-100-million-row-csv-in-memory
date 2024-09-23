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
    memoryStats.push(getMemoryStats(0));
    
    const nRows = 100_000_000;
    const chemMap: { [key: number]: string } = {};
    const reverseChemMap: { [key: string]: number } = {};
    let chemIndex = 0;

    const shipperMap: { [key: number]: string } = {};
    const reverseShipperMap: { [key: string]: number } = {};
    let shipperIndex = 0;

    const buffer = new ArrayBuffer(nRows * (4 + 8 + 4 + 4 + 1));
    const chemArray = new Uint32Array(buffer, 0, nRows);
    const amountArray = new Float64Array(buffer, nRows * 4, nRows);
    const shipperArray = new Uint32Array(buffer, nRows * 12, nRows);
    const shippedAtArray = new Uint32Array(buffer, nRows * 16, nRows);
    const nullTrackingArray = new Uint8Array(buffer, nRows * 20, nRows);

    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);
    let rowIndex = 0;

    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser()).on('data', (row: any) => {
            let nullByte = 0;
            if (row.chem_name === null || row.chem_name === '') {
                nullByte |= (1 << 0);
            } else {
                if (!(row.chem_name in reverseChemMap)) {
                    reverseChemMap[row.chem_name] = chemIndex;
                    chemMap[chemIndex] = row.chem_name;
                    chemIndex++;
                }
                chemArray[rowIndex] = reverseChemMap[row.chem_name];
            }

            if (row.amount === null || row.amount === '') {
                nullByte |= (1 << 1);
            } else {
                amountArray[rowIndex] = parseFloat(row.amount);
            }

            if (row.shipper === null || row.shipper === '') {
                nullByte |= (1 << 2);
            } else {
                if (!(row.shipper in reverseShipperMap)) {
                    reverseShipperMap[row.shipper] = shipperIndex;
                    shipperMap[shipperIndex] = row.shipper;
                    shipperIndex++;
                }
                shipperArray[rowIndex] = reverseShipperMap[row.shipper];
            }

            if (row.shipped_at === null || row.shipped_at === '') {
                nullByte |= (1 << 3);
            } else {
                shippedAtArray[rowIndex] = parseInt(row.shipped_at, 10);
            }

            nullTrackingArray[rowIndex] = nullByte;
            rowIndex++;
        })
        .on('end', () => resolve())
        .on('error', (error) => reject(error));
    });

    memoryStats.push(getMemoryStats(nRows));
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/arrayBuffer_${nRows}.csv`);
}

main();