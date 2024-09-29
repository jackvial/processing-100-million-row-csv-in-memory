import { parentPort, workerData } from 'worker_threads';

// Access shared typed arrays
const chemArray = new Uint8Array(workerData.chemArrayBuffer);
const amountArray = new Float32Array(workerData.amountArrayBuffer);
const shipperArray = new Uint8Array(workerData.shipperArrayBuffer);
const shippedAtArray = new Uint32Array(workerData.shippedAtArrayBuffer);

// Access atomic row index
const rowIndex = new Int32Array(workerData.rowIndexBuffer);

// Function to split Uint8Array on a delimiter (comma in this case)
function splitBuffer(buffer, delimiter) {
  const result = [];
  let start = 0;
  let index;

  while ((index = buffer.indexOf(delimiter, start)) !== -1) {
    result.push(buffer.slice(start, index));
    start = index + 1;
  }
  // Push the last segment
  result.push(buffer.slice(start));
  return result;
}

// Function to convert a Uint8Array to string
function bufferToString(buffer) {
  return Buffer.from(buffer).toString('utf8');
}

parentPort.on('message', (msg) => {
  if (msg.done) {
    // Close the worker when done
    parentPort.close();
  } else if (msg.lines) {
    const lines = msg.lines;
    for (const lineBuffer of lines) {
      // Safely increment the index
      const index = Atomics.add(rowIndex, 0, 1);

      // Split the line buffer on commas (ASCII code 44)
      const fields = splitBuffer(lineBuffer, 44); // 44 is ','

      // Assume fields: [chem, amount, shipper, shippedAt]
      // Convert fields as needed

      // Example for 'chem' field (assuming single-byte representation)
      // You may need to adjust based on actual data
      chemArray[index] = fields[0][0]; // Taking the first byte

      // Convert 'amount' field from bytes to float
      const amountString = bufferToString(fields[1]);
      amountArray[index] = parseFloat(amountString);

      // Example for 'shipper' field
      shipperArray[index] = fields[2][0]; // Taking the first byte

      // Convert 'shippedAt' field from bytes to integer
      const shippedAtString = bufferToString(fields[3]);
      shippedAtArray[index] = parseInt(shippedAtString, 10);
    }
    // Notify the main thread that processing is complete
    parentPort.postMessage({ processed: true });
  }
});