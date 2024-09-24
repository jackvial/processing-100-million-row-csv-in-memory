import fs from 'fs';
import {
    getMemoryStats,
    MemoryStatsRow,
    writeStatsToCsv
} from "../utils";
import { Transform } from 'stream';

const [cr] = Buffer.from('\r');
const [nl] = Buffer.from('\n');
const separator = Buffer.from(',')[0];  // comma
const quote = Buffer.from('"')[0];      // quote character

export async function main() {
    const startTime = Date.now();
    const memoryStats: MemoryStatsRow[] = [];
    memoryStats.push(getMemoryStats(0));

    const nRows = 100_000_000;
    const buffer = new ArrayBuffer(nRows * (1 + 4 + 1 + 4));
    const chemArray = new Uint8Array(buffer, 0, nRows);
    const amountArray = new Float32Array(buffer, nRows, nRows);
    const shipperArray = new Uint8Array(buffer, nRows * 3, nRows);
    const shippedAtArray = new Uint32Array(buffer, nRows * 4, nRows);

    const chemMap: { [key: number]: string } = {};
    const reverseChemMap: { [key: string]: number } = {};
    let chemIndex = 0;

    const shipperMap: { [key: number]: string } = {};
    const reverseShipperMap: { [key: string]: number } = {};
    let shipperIndex = 0;

    const filePath = 'outputs/chemicals_shipped_100000000.csv';
    const readStream = fs.createReadStream(filePath);
    let rowIndex = 0;

    class CsvParser extends Transform {
        private prev: Buffer | null = null;
        private quoted: boolean = false;

        constructor() {
            super({ readableObjectMode: false });
        }

        _transform(chunk: Buffer, encoding: BufferEncoding, callback: Function) {
            let buffer = chunk;
            if (this.prev) {
                buffer = Buffer.concat([this.prev, chunk]);
                this.prev = null;
            }

            let start = 0;
            const length = buffer.length;

            for (let i = 0; i < length; i++) {
                if (buffer[i] === nl || buffer[i] === cr) {
                    if (!this.quoted) {
                        this.parseLine(buffer.slice(start, i));
                        start = i + 1;
                    }
                } else if (buffer[i] === quote) {
                    this.quoted = !this.quoted;
                }
            }

            if (start < length) {
                this.prev = buffer.slice(start);  // store remaining part for next chunk
            }

            // Log memory stats every 1M rows
            if (rowIndex % 1_000_000 === 0) {
                memoryStats.push(getMemoryStats(rowIndex));
            }
            callback();
        }

        parseLine(lineBuffer: Buffer) {
            let cells = [];
            let start = 0;
            let quoted = false;

            for (let i = 0; i <= lineBuffer.length; i++) {
                if (i === lineBuffer.length || (!quoted && lineBuffer[i] === separator)) {
                    cells.push(lineBuffer.slice(start, i).toString('utf-8').replace(/(^"|"$)/g, ''));  // Strip quotes
                    start = i + 1;
                } else if (lineBuffer[i] === quote) {
                    quoted = !quoted;
                }
            }

            const chem_name = cells[0];
            const amount = parseFloat(cells[1]);
            const shipper = cells[2];
            const shipped_at = parseInt(cells[3], 10);

            if (!(chem_name in reverseChemMap)) {
                reverseChemMap[chem_name] = chemIndex;
                chemMap[chemIndex] = chem_name;
                chemIndex++;
            }

            if (!(shipper in reverseShipperMap)) {
                reverseShipperMap[shipper] = shipperIndex;
                shipperMap[shipperIndex] = shipper;
                shipperIndex++;
            }

            chemArray[rowIndex] = reverseChemMap[chem_name];
            amountArray[rowIndex] = amount;
            shipperArray[rowIndex] = reverseShipperMap[shipper];
            shippedAtArray[rowIndex] = shipped_at;

            rowIndex++;

            console.log(rowIndex);
        }
    }

    await new Promise<void>((resolve, reject) => {
        const csvParser = new CsvParser();
        readStream
            .pipe(csvParser)
            .on('finish', resolve)
            .on('error', reject);
    });

    memoryStats.push(getMemoryStats(nRows));
    writeStatsToCsv({
        memoryStats,
        duration: Date.now() - startTime
    }, `stats/fasterCsvParser_${nRows}.csv`);
}

main();
