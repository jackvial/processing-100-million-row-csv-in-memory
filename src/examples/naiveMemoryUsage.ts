import {
    prettyPrintMemoryUsage,
} from "../utils"

function main () {
    const nRows = 10_000_000;
    prettyPrintMemoryUsage({
        nRows
    });

    const rows: any[] = [];
    for (let i = 0; i < nRows; i++) {
        rows.push({
            SKU: i % 10,
            price: i * 0.01,
            isAvailable: i % 2 === 1,
            color: ['red', 'green', 'blue', 'purple'][i % 4]
        });
    }

    prettyPrintMemoryUsage({
        nRows
    });

    // Group price by color and sum each group use plain for loop
    const priceByColor: { [key: string]: number } = {};
    for (let i = 0; i < nRows; i++) {
        const row = rows[i];
        const price = row.price;
        const color = row.color;
        if (!priceByColor[color]) {
            priceByColor[color] = 0;
        }
        priceByColor[color] += price;
    }

    console.log(priceByColor);

    prettyPrintMemoryUsage({
        nRows
    });
}

main();