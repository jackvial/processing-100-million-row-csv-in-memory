export function main () {
    const nRows = 100_000_000;
    const colorMap = {
        0: 'red',
        1: 'green',
        2: 'blue',
        3: 'purple'
    };

    // SKU (4 bytes), price (4 bytes), isAvailable (1 byte), color (1 byte)
    const buffer = new ArrayBuffer(nRows * (4 + 4 + 1 + 1));

    const skuArray = new Uint32Array(buffer, 0, nRows);
    const priceArray = new Float32Array(buffer, nRows * 4, nRows);
    const isAvailableArray = new Uint8Array(buffer, nRows * 8, nRows);
    const colorArray = new Uint8Array(buffer, nRows * 9, nRows);

    for (let i = 0; i < nRows; i++) {
        skuArray[i] = i % 10;                 // SKU is an integer
        priceArray[i] = i * 0.01;             // Price is a float
        isAvailableArray[i] = i % 2;          // Boolean stored as 1 or 0
        colorArray[i] = i % 4;                // Store color index (0-3)
    }

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
}

main();