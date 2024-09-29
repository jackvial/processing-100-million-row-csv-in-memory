// main.ts
import fs from 'fs';
import { Worker } from 'worker_threads';
import LineSplitter from './lineSplitterTransformer';
import { getMemoryStats } from '../utils';
import { Buffer } from 'node:buffer';

console.time('CSV Parser');

const N_WORKERS = 5;
const DATA_BUFFER_SIZE = 128 * 1024 * 1024;
const MAX_LINES_PER_WORKER = 25_000_000; // Adjust based on expected lines per worker

// Shared arrays for the final parsed data
const rowCount = 100_000_000;
const chemArray = new Uint8Array(new SharedArrayBuffer(rowCount));
const amountArray = new Float32Array(new SharedArrayBuffer(rowCount * 4));
const shipperArray = new Uint8Array(new SharedArrayBuffer(rowCount));
const shippedAtArray = new Uint32Array(new SharedArrayBuffer(rowCount * 4));

interface WorkerData {
    worker: Worker;
    dataBuffer: SharedArrayBuffer;
    dataView: Uint8Array;
    lineOffsets: Int32Array;
    lineLengths: Int32Array;
    writeIndex: Int32Array;
    currentOffset: number;
}

const workers: WorkerData[] = [];

for (let i = 0; i < N_WORKERS; i++) {
    // Data buffer to hold concatenated line data
    const dataBuffer = new SharedArrayBuffer(DATA_BUFFER_SIZE);
    const dataView = new Uint8Array(dataBuffer);

    // Buffer to hold line offsets (start positions)
    const lineOffsetsBuffer = new SharedArrayBuffer(MAX_LINES_PER_WORKER * 4); // Int32Array for offsets
    const lineOffsets = new Int32Array(lineOffsetsBuffer);

    // Buffer to hold line lengths
    const lineLengthsBuffer = new SharedArrayBuffer(MAX_LINES_PER_WORKER * 4); // Int32Array for lengths
    const lineLengths = new Int32Array(lineLengthsBuffer);

    // Atomic write index (number of lines written)
    const writeIndexBuffer = new SharedArrayBuffer(4);
    const writeIndex = new Int32Array(writeIndexBuffer);
    writeIndex[0] = 0;

    // Create worker
    const worker = new Worker('./dist/multithreadedCsvLoader/worker.js', {
        workerData: {
            dataBuffer,
            lineOffsetsBuffer,
            lineLengthsBuffer,
            writeIndexBuffer,
            workerId: i,
            chemArrayBuffer: chemArray.buffer,
            amountArrayBuffer: amountArray.buffer,
            shipperArrayBuffer: shipperArray.buffer,
            shippedAtArrayBuffer: shippedAtArray.buffer,
            rowCount,
        },
    });

    workers.push({
        worker,
        dataBuffer,
        dataView,
        lineOffsets,
        lineLengths,
        writeIndex,
        currentOffset: 0,
    });
}

// Read the file and process lines
const filePath = 'outputs/chemicals_shipped_100000000.csv';
const readStream = fs.createReadStream(filePath, { highWaterMark: 24 * 1024 * 1024 });
const lineSplitter = new LineSplitter();

const lineStream = readStream.pipe(lineSplitter);

let workerIndex = 0;

lineStream.on('data', (lineBuffer: Buffer) => {
    // Get the current worker
    const workerData = workers[workerIndex];

    const { dataView, lineOffsets, lineLengths, writeIndex } = workerData;

    // Get the line length
    const lineLength = lineBuffer.length;

    // Check if there is enough space in the dataBuffer
    if (workerData.currentOffset + lineLength > DATA_BUFFER_SIZE) {
        // Buffer is full; switch to the next worker
        workerIndex = (workerIndex + 1) % N_WORKERS;
        return;
    }

    // Get the current writeIndex
    const idx = Atomics.load(writeIndex, 0);

    // Check if lineOffsets buffer has space
    if (idx >= MAX_LINES_PER_WORKER) {
        // Offset buffer is full; switch to the next worker
        workerIndex = (workerIndex + 1) % N_WORKERS;
        return;
    }

    // Record the start position and length of the line
    lineOffsets[idx] = workerData.currentOffset;
    lineLengths[idx] = lineLength;

    // Copy the line data into dataBuffer
    dataView.set(lineBuffer, workerData.currentOffset);

    // Update the current offset
    workerData.currentOffset += lineLength;

    // Atomically increment the writeIndex
    Atomics.add(writeIndex, 0, 1);

    // Round-robin assignment to workers
    workerIndex = (workerIndex + 1) % N_WORKERS;
});

lineStream.on('end', () => {
    // Signal to workers that processing is done
    for (const workerData of workers) {
        const { worker, currentOffset } = workerData;
        worker.postMessage({ done: true, totalDataLength: currentOffset });
    }
});

// Handle worker messages
let workersDone = 0;
for (const workerData of workers) {
    const { worker } = workerData;
    worker.on('message', (msg: any) => {
        if (msg.done) {
            console.log(`Worker ${msg.workerId} done processing`);
            workersDone++;

            if (workersDone === N_WORKERS) {

                // print slice of each array
                // console.log(chemArray.slice(0, 5));
                // console.log(amountArray.slice(0, 5));
                // console.log(shipperArray.slice(0, 5));
                // console.log(shippedAtArray.slice(0, 5));

                // log the last 5 elements of each array
                // find index of first element with 0 value in amount array
                let index = amountArray.findIndex((element) => element === 0);
                console.log(index);
                console.log(amountArray.slice(index - 5, index));
                console.log(amountArray.slice(index, index + 5));

                getMemoryStats(100_000_000);

                console.timeEnd('CSV Parser');
            }
        }
    });

    worker.on('error', (err: Error) => {
        console.error(`Worker error: ${err}`);
    });
}
