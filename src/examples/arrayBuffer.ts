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
    const buffer = new ArrayBuffer(nRows * (4 + 8 + 4 + 4));
    const chemArray = new Uint32Array(buffer, 0, nRows);
    const amountArray = new Float64Array(buffer, nRows * 4, nRows);
    const shipperArray = new Uint32Array(buffer, nRows * 12, nRows);
    const shippedAtArray = new Uint32Array(buffer, nRows * 16, nRows);
    
    let rowIndex = 0;
    const chemMap: { [key: number]: string } = {};
    const reverseChemMap: { [key: string]: number } = {};
    let chemIndex = 0;

    const shipperMap: { [key: number]: string } = {};
    const reverseShipperMap: { [key: string]: number } = {};
    let shipperIndex = 0;

    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);
    await new Promise<void>((resolve, reject) => {
        readStream.pipe(csvParser()).on('data', (row: any) => {
            if (!(row.chem_name in reverseChemMap)) {
                reverseChemMap[row.chem_name] = chemIndex;
                chemMap[chemIndex] = row.chem_name;
                chemIndex++;
            }

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
            shippedAtArray[rowIndex] = parseInt(row.shipped_at, 10);

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
