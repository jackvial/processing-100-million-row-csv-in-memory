import {
    prettyPrintMemoryUsage,
} from "../utils"

function main () {
    const nRows = 100_000_000;
    prettyPrintMemoryUsage({
        nRows
    });

    // Define constants for colors
    const colorMap = {
        0: 'red',
        1: 'green',
        2: 'blue',
        3: 'purple'
    };

    console.time('Create Buffer');

    // Create a single ArrayBuffer large enough to hold all data
    const buffer = new ArrayBuffer(nRows * (4 + 4 + 1 + 1)); // SKU (4 bytes), price (4 bytes), isAvailable (1 byte), color (1 byte)

    // Create TypedArray views over the ArrayBuffer
    const skuArray = new Uint32Array(buffer, 0, nRows);       // 4 bytes per entry
    const priceArray = new Float32Array(buffer, nRows * 4, nRows);  // 4 bytes per entry
    const isAvailableArray = new Uint8Array(buffer, nRows * 8, nRows);  // 1 byte per entry
    const colorArray = new Uint8Array(buffer, nRows * 9, nRows);  // 1 byte per entry (store color as a number)

    // Populate data
    for (let i = 0; i < nRows; i++) {
        skuArray[i] = i % 10;                 // SKU is an integer
        priceArray[i] = i * 0.01;             // Price is a float
        isAvailableArray[i] = i % 2;          // Boolean stored as 1 or 0
        colorArray[i] = i % 4;                // Store color index (0-3)

        // Print memory usage every 1 million rows
        if (i % 1_000_000 === 0) {
            prettyPrintMemoryUsage({
                nRows: i
            });
        }
    }

    
    prettyPrintMemoryUsage({
        nRows
    });

    console.timeEnd('Create Buffer');
    console.log('---------------------------------');
    console.time('Group Price By Color');

    // Drop duplicates my selected columns
    const dropDuplicateCols: string[] = ['SKU', 'color'];
    const dupeHashes = new Set<string>();
    const dupeIndexes: number[] = [];
    for (let i = 0; i < nRows; i++) {
        const hash = `${skuArray[i]}_${colorArray[i]}`;
        if (dupeHashes.has(hash)) {
            dupeIndexes.push(i);
        } else {
            dupeHashes.add(hash);
        }
    }

    console.log(`Found ${dupeIndexes.length} duplicates`);

    // Remove duplicates
    const nUniqueRows = nRows - dupeIndexes.length;
    const uniqueBuffer = new ArrayBuffer(nUniqueRows * (4 + 4 + 1 + 1));
    const uniqueSkuArray = new Uint32Array(uniqueBuffer, 0, nUniqueRows);
    const uniquePriceArray = new Float32Array(uniqueBuffer, nUniqueRows * 4, nUniqueRows);
    const uniqueIsAvailableArray = new Uint8Array(uniqueBuffer, nUniqueRows * 8, nUniqueRows);
    const uniqueColorArray = new Uint8Array(uniqueBuffer, nUniqueRows * 9, nUniqueRows);

    let uniqueRowIndex = 0;
    for (let i = 0; i < nRows; i++) {
        if (!dupeIndexes.includes(i)) {
            uniqueSkuArray[uniqueRowIndex] = skuArray[i];
            uniquePriceArray[uniqueRowIndex] = priceArray[i];
            uniqueIsAvailableArray[uniqueRowIndex] = isAvailableArray[i];
            uniqueColorArray[uniqueRowIndex] = colorArray[i];
            uniqueRowIndex++;
        }
    }

    console.log(`Unique rows: ${nUniqueRows}`);

    prettyPrintMemoryUsage({
        nRows: nUniqueRows
    });

    // Group price by color and sum each group using a for loop, no object copy
    const priceByColor: { [key: string]: number } = {
        red: 0,
        green: 0,
        blue: 0,
        purple: 0
    };

    for (let i = 0; i < nRows; i++) {
        const price = priceArray[i];
        const colorIndex = colorArray[i];
        const color = colorMap[colorIndex];
        priceByColor[color] += price;
    }

    console.log(priceByColor);

    prettyPrintMemoryUsage({
        nRows
    });

    console.log('---------------------------------');
    console.timeEnd('Group Price By Color');
}

main();