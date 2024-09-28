import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import fs from 'fs';
import { promisify } from 'util';
import { ReadStream } from 'fs';
import {
    getMemoryStats
} from "../utils"

const stat = promisify(fs.stat);

async function createMultipleReadStreams(fileStats: fs.Stats, filePath: string, numChunks: number): Promise<ReadStream[]> {
    const fileSize = fileStats.size;
    const chunkSize = Math.ceil(fileSize / numChunks);
    const readStreams: ReadStream[] = [];

    for (let i = 0; i < numChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min((i + 1) * chunkSize, fileSize) - 1;
        const readStream = fs.createReadStream(filePath, { start, end, highWaterMark: 1024 * 1024 * 32 });
        readStreams.push(readStream);
    }

    return readStreams;
}

async function processStream(stream: ReadStream, index: number, sharedBuffer: SharedArrayBuffer, offset: number): Promise<void> {
    return new Promise((resolve, reject) => {
        const view = new Uint8Array(sharedBuffer, offset);
        let position = 0;

        stream.on('data', (chunk: Buffer) => {
            view.set(chunk, position);
            position += chunk.length;
        });

        stream.on('end', () => {
            console.log(`Stream ${index + 1} ended. Bytes read: ${position}`);
            resolve();
        });

        stream.on('error', (error) => {
            reject(error);
        });
    });
}

function bytesToGB(bytes: number): number {
    return Number((bytes / Math.pow(2, 30)).toFixed(4));
}

function countLinesInChunk(chunk: Uint8Array): number {
    let count = 0;
    for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] === 10) { // ASCII for '\n'
            count++;
        }
    }
    return count;
}

function parseFloatFromBytes(bytes: Uint8Array): number {
    let result = 0;
    let factor = 1;
    let decimalFound = false;
    let decimalFactor = 1;

    for (let i = 0; i < bytes.length; i++) {
        const byte = bytes[i];

        if (byte === 46) { // ASCII for '.'
            decimalFound = true;
            continue;
        }

        const digit = byte - 48; // Convert ASCII '0'-'9' to 0-9

        if (digit >= 0 && digit <= 9) {
            if (decimalFound) {
                decimalFactor *= 10;
                result += digit / decimalFactor;
            } else {
                result = result * 10 + digit;
            }
        }
    }

    return result * factor;
}

function parseCsvFromBuffer(buffer: SharedArrayBuffer, nRows: number): {
    lineCount: number;
    commaCounts: number[];
} {
    const view = new Uint8Array(buffer);
    const chunkSize = 1024 * 1024;

    // The target ArrayBuffer and typed arrays
    const rowBuffer = new ArrayBuffer(nRows * (1 + 4 + 1 + 4));
    const chemArray = new Uint8Array(rowBuffer, 0, nRows);
    const amountArray = new Float32Array(rowBuffer, nRows, nRows);
    const shipperArray = new Uint8Array(rowBuffer, nRows * 3, nRows);
    const shippedAtArray = new Uint32Array(rowBuffer, nRows * 4, nRows);

    const chemMap: { [key: number]: string } = {};
    const reverseChemMap: { [key: string]: number } = {};
    let chemIndex = 0;

    const shipperMap: { [key: number]: string } = {};
    const reverseShipperMap: { [key: string]: number } = {};
    let shipperIndex = 0;

    let lineCount = 0;
    let currentLineCommaCount = 0;
    let rowStart = 0; // Track where each row starts
    let currentField = '';
    let currentFieldStart = 0; // Track the start of the current field

    const commaCounts = [];

    for (let i = 0; i < view.length; i += chunkSize) {
        const chunk = view.subarray(i, Math.min(i + chunkSize, view.length));

        for (let j = 0; j < chunk.length; j++) {
            const char = chunk[j];

            if (char === 44) { // ASCII for ','
                currentLineCommaCount++;
                // Process the field based on column position (assume: [0]chem, [1]amount, [2]shipper, [3]shippedAt)
                // if (currentLineCommaCount === 1) {
                //     // Process 'chem'
                //     // if (!(currentField in reverseChemMap)) {
                //     //     reverseChemMap[currentField] = chemIndex;
                //     //     chemMap[chemIndex] = currentField;
                //     //     chemIndex++;
                //     // }
                //     // chemArray[lineCount] = reverseChemMap[currentField];
                if (currentLineCommaCount === 2) {
                    amountArray[lineCount] = Number(currentField);
                    // const fieldData = view.subarray(currentFieldStart, j); // Get the byte slice for the 'amount' field
                    // amountArray[lineCount] = parseFloatFromBytes(fieldData); // Use the custom byte-to-float function
                } 
                // else if (currentLineCommaCount === 3) {
                // //     // Process 'shipper'
                // //     // if (!(currentField in reverseShipperMap)) {
                // //     //     reverseShipperMap[currentField] = shipperIndex;
                // //     //     shipperMap[shipperIndex] = currentField;
                // //     //     shipperIndex++;
                // //     // }
                // //     // shipperArray[lineCount] = reverseShipperMap[currentField];
                //     currentFieldStart = j + 1; // Move the start to after the comma
                // }
            } else if (char === 10) { // ASCII for '\n'
                // Process 'shippedAt' (last column)
                // shippedAtArray[lineCount] = parseInt(currentField);

                // // Finalize row parsing
                lineCount++;
                // commaCounts.push(currentLineCommaCount);
                // currentLineCommaCount = 0; // Reset comma count for the next line
                currentField = ''; // Reset for the next row
                // currentFieldStart = j + 1; // Move start to the next row
            } else {
                // Append character to current field
                currentField += String.fromCharCode(char);
            }
        }
    }

    // Handle the last line if it doesn't end with a newline
    if (currentField.length > 0) {
        shippedAtArray[lineCount] = parseInt(currentField);
        commaCounts.push(currentLineCommaCount);
        lineCount++;
    }

    return {
        lineCount,
        commaCounts,
    };
}

if (isMainThread) {
    async function main(): Promise<void> {
        const filePath = 'outputs/chemicals_shipped_100000000.csv';
        const numChunks = 6;

        try {
            const fileStats = await stat(filePath);
            console.log('fileStats:', fileStats);
            console.log('File size:', fileStats.size);
            console.log('File size in GB:', bytesToGB(fileStats.size));

            const startTime = Date.now();

            // Create a SharedArrayBuffer to store the entire file content
            const sharedBuffer = new SharedArrayBuffer(fileStats.size);

            const streams = await createMultipleReadStreams(fileStats, filePath, numChunks);

            const chunkSize = Math.ceil(fileStats.size / numChunks);
            const streamPromises = streams.map((stream, index) =>
                processStream(stream, index, sharedBuffer, index * chunkSize)
            );

            await Promise.all(streamPromises);

            console.log(`All streams finished. Total bytes read: ${fileStats.size}`);
            console.log(`GB read:`, bytesToGB(fileStats.size));
            console.log('Time taken to read file:', Date.now() - startTime);

            // Example: Read first 100 bytes from the shared buffer
            const sampleView = new Uint8Array(sharedBuffer, 0, 100);
            console.log('First 100 bytes:', Buffer.from(sampleView).toString());

            // Count lines in the shared buffer
            console.time('Parse CSV');
            // const lineCount = countLines(sharedBuffer);
            const result = parseCsvFromBuffer(sharedBuffer, 100_000_000);
            console.timeEnd('Parse CSV');
            console.log('Line count:', result.lineCount);
            console.log('Comma counts:', result.commaCounts.length);

            getMemoryStats(100_000_000);

        } catch (error) {
            console.error('Error:', error);
        }
    }

    main();
} else {
    // This code runs in worker threads
    const { buffer, start, end } = workerData;
    const chunk = new Uint8Array(buffer, start, end - start);
    const lineCount = countLinesInChunk(chunk);
    parentPort?.postMessage(lineCount);
}