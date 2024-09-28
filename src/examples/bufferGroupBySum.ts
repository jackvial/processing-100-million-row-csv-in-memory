import { getMemoryStats } from '../utils';
import { Buffer } from 'node:buffer';

// Utility function to get random float between min and max
function getRandomFloat(min, max) {
    return Math.random() * (max - min) + min;
}

function main() {
    // Simulate loading of 10 million rows of data
    const nRows = 10_000_000;

    // Buffer to store SKU (int), price (float), isAvailable (boolean), color (int)
    const buffer = Buffer.alloc(nRows * 16); // 4 bytes for SKU, 8 bytes for price, 1 byte for isAvailable, 3 bytes padding for alignment, 4 bytes for color index
    const skus = new Int32Array(buffer, 0, nRows); // 4 bytes per int
    const prices = new Float32Array(buffer, nRows * 4, nRows); // 8 bytes per float
    const isAvailable = new Uint8Array(buffer, nRows * 12, nRows); // 1 byte for boolean
    const colorIndexes = new Uint32Array(buffer, nRows * 13, nRows); // 4 bytes for color index

    const colors = ['red', 'blue', 'green', 'yellow'];

    // Fill the buffer with random data
    for (let i = 0; i < nRows; i++) {
        skus[i] = i % 100;
        prices[i] = getRandomFloat(0, 100);
        isAvailable[i] = Math.random() > 0.5 ? 1 : 0;
        colorIndexes[i] = i % 4; // Use an index for colors (0 = red, 1 = blue, etc.)
    }

    // Group data by color index
    const groupedData = {
        0: { color: 'red', sumPrice: 0, count: 0 },
        1: { color: 'blue', sumPrice: 0, count: 0 },
        2: { color: 'green', sumPrice: 0, count: 0 },
        3: { color: 'yellow', sumPrice: 0, count: 0 },
    };

    // Sum the prices by color
    for (let i = 0; i < nRows; i++) {
        const colorIndex = colorIndexes[i];
        groupedData[colorIndex].sumPrice += prices[i];
        groupedData[colorIndex].count++;
    }

    // Display results
    const summed = Object.keys(groupedData).reduce((result, key) => {
        result[groupedData[key].color] = groupedData[key].sumPrice;
        return result;
    }, {});

    console.log(summed); // Example output: { red: 1245231.34, blue: 1034532.12, ... }

    // Output memory usage stats
    console.log(getMemoryStats(nRows));
}

main();
