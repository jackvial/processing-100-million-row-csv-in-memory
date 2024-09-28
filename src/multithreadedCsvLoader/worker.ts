// worker.ts
import { parentPort, workerData } from 'worker_threads';

interface WorkerData {
  dataBuffer: SharedArrayBuffer;
  lineOffsetsBuffer: SharedArrayBuffer;
  lineLengthsBuffer: SharedArrayBuffer;
  writeIndexBuffer: SharedArrayBuffer;
  workerId: number;
  chemArrayBuffer: SharedArrayBuffer;
  amountArrayBuffer: SharedArrayBuffer;
  shipperArrayBuffer: SharedArrayBuffer;
  shippedAtArrayBuffer: SharedArrayBuffer;
  rowCount: number;
}

const {
  dataBuffer,
  lineOffsetsBuffer,
  lineLengthsBuffer,
  writeIndexBuffer,
  workerId,
  chemArrayBuffer,
  amountArrayBuffer,
  shipperArrayBuffer,
  shippedAtArrayBuffer,
  rowCount,
} = workerData as WorkerData;

const dataView = new Uint8Array(dataBuffer);
const lineOffsets = new Int32Array(lineOffsetsBuffer);
const lineLengths = new Int32Array(lineLengthsBuffer);
const writeIndex = new Int32Array(writeIndexBuffer);

// Access shared typed arrays
const chemArray = new Uint8Array(chemArrayBuffer);
const amountArray = new Float32Array(amountArrayBuffer);
const shipperArray = new Uint8Array(shipperArrayBuffer);
const shippedAtArray = new Uint32Array(shippedAtArrayBuffer);

// Initialize atomic read index
let readIndex = 0;
let totalDataLength: number | undefined;

function processLines() {
  const currentWriteIndex = Atomics.load(writeIndex, 0);

  while (readIndex < currentWriteIndex) {
    const start = lineOffsets[readIndex];
    const length = lineLengths[readIndex];
    const end = start + length;

    // Check if the end is within the dataView
    if (end > dataView.length) {
      // Wait for more data
      break;
    }

    const lineData = dataView.subarray(start, end);

    // Parse the line data
    parseLine(lineData, readIndex);

    readIndex++;
  }
}

function parseLine(lineData: Uint8Array, index: number) {
  // Split the line on commas (ASCII code 44)
  const fields: Uint8Array[] = [];
  let fieldStart = 0;

  for (let i = 0; i <= lineData.length; i++) {
    if (i === lineData.length || lineData[i] === 44) {
      // Field delimiter or end of line
      const field = lineData.subarray(fieldStart, i);
      fields.push(field);
      fieldStart = i + 1;
    }
  }

  // Now fields is an array of Uint8Array for each field
  // Process each field accordingly

  // Example for 'chem' field (assuming single-byte representation)
  chemArray[index] = fields[0][0]; // Adjust based on actual data

  // Convert 'amount' field from bytes to float
  const amountString = bufferToString(fields[1]);
  amountArray[index] = parseFloat(amountString);

  // Example for 'shipper' field
  shipperArray[index] = fields[2][0]; // Adjust based on actual data

  // Convert 'shippedAt' field from bytes to integer
  const shippedAtString = bufferToString(fields[3]);
  shippedAtArray[index] = parseInt(shippedAtString, 10);
}

function bufferToString(buffer: Uint8Array): string {
  return Buffer.from(buffer).toString('utf8');
}

// Periodically check for new data
const interval = setInterval(processLines, 100);

parentPort?.on('message', (msg: any) => {
  if (msg.done) {
    totalDataLength = msg.totalDataLength;

    // Process any remaining lines
    processLines();

    // Stop the interval
    clearInterval(interval);

    parentPort?.postMessage({ done: true, workerId });
  }
});
