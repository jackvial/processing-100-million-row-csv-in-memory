function bytesToMB(bytes) {
    return (bytes / 1024 / 1024).toFixed(4);
}

function prettyPrintMemoryUsage() {
    console.log('----------------------------------------');
    const memoryUsage = process.memoryUsage();

    console.log('Memory Usage:');
    console.log(`RSS: ${bytesToMB(memoryUsage.rss)} MB`);
    console.log(`Heap Total: ${bytesToMB(memoryUsage.heapTotal)} MB`);
    console.log(`Heap Used: ${bytesToMB(memoryUsage.heapUsed)} MB`);
    console.log(`External: ${bytesToMB(memoryUsage.external)} MB`);
    console.log(`Array Buffers: ${bytesToMB(memoryUsage.arrayBuffers)} MB`);
}

// Print initial memory usage
prettyPrintMemoryUsage();

const bufBytes = 2 ** 30;
console.log(`Allocate ${bytesToMB(bufBytes)} MB buffer`);
const buffer = new ArrayBuffer(bufBytes);
const float32BufferView = new Float32Array(buffer);  // 32-bit float view on buffer

// Print memory usage after buffer allocation
prettyPrintMemoryUsage();

// Populate buffer with 32-bit floats
console.log('Populating the buffer with 32-bit floats...');
for (let i = 0; i < float32BufferView.length; i++) {
    float32BufferView[i] = i / float32BufferView.length;  // Fill buffer with fractional values
}

// Sum all items in the buffer
let sum = 0;
for (let i = 0; i < float32BufferView.length; i++) {
    sum += float32BufferView[i];
}

console.log(`Sum of all items in the buffer: ${sum}`);

// Print memory usage after populating and summing buffer
prettyPrintMemoryUsage();