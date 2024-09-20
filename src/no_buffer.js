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

const arraySize = 2 ** 28;  // Equivalent to 1 MB for 32-bit floats (262144 elements)
console.log(`Allocate array of size ${arraySize} (each float 32-bit)`);

// Create a regular JavaScript array to simulate 32-bit float storage
const float32Array = new Array(arraySize);

// Print memory usage after array allocation
prettyPrintMemoryUsage();

// Populate the array with 32-bit floats
console.log('Populating the array with 32-bit floats...');
for (let i = 0; i < float32Array.length; i++) {
    float32Array[i] = i / float32Array.length;  // Fill array with fractional values
}

// Sum all items in the array
let sum = 0;
for (let i = 0; i < float32Array.length; i++) {
    sum += float32Array[i];
}

console.log(`Sum of all items in the array: ${sum}`);

// Print memory usage after populating and summing the array
prettyPrintMemoryUsage();