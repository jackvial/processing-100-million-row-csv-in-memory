import { Column, ZeroCopyDataFrame } from '../df';
import { prettyPrintMemoryUsage } from '../utils';
import { exportToCSV } from '../csv';

function main() {
    const nRows = 10_000_000;
    prettyPrintMemoryUsage({
        nRows
    });

    // Calculate buffer size for each column based on row count
    const intColSize = nRows * 4;  // 4 bytes per int32 value
    const floatColSize = nRows * 4;  // 4 bytes per float32 value
    const boolColSize = nRows * 1;  // 1 byte per bool value
    const stringColSize = nRows * 1;  // 1 byte to store the string index

    console.log('Number of rows for each type:');
    console.log(`int32: ${nRows}`);
    console.log(`float32: ${nRows}`);
    console.log(`bool: ${nRows}`);
    console.log(`string: ${nRows}`);

    // Create buffers for each column.
    const intBuffer = new ArrayBuffer(intColSize);
    const floatBuffer = new ArrayBuffer(floatColSize);
    const boolBuffer = new ArrayBuffer(boolColSize);
    const stringBuffer = new ArrayBuffer(stringColSize);

    // Create DataView instances
    const int32View = new DataView(intBuffer);
    const float32View = new DataView(floatBuffer);
    const boolView = new DataView(boolBuffer);
    const stringView = new DataView(stringBuffer);

    // Populate the entire int32 column with values (filling all rows)
    for (let i = 0; i < nRows; i++) {
        int32View.setInt32(i * 4, i % 10, true);  // Store values 0 to 9 repeatedly (example)
    }

    // Populate the entire float32 column with values (filling all rows)
    for (let i = 0; i < nRows; i++) {
        float32View.setFloat32(i * 4, i * 0.01, true);  // Store floating-point values
    }

    // Populate the entire bool column with alternating true/false (1/0)
    for (let i = 0; i < nRows; i++) {
        boolView.setUint8(i, i % 2);  // Alternate between 1 (true) and 0 (false)
    }

    // Populate the string column with "red", "green", "blue", "purple" (filling all rows)
    const stringValues = ['red', 'green', 'blue', 'purple'];
    for (let i = 0; i < nRows; i++) {
        stringView.setUint8(i, i % 4);  // Store the index for "red", "green", "blue", "purple"
    }

    // Define columns with metadata
    const columns: Column[] = [
        {
            name: 'SKU',
            dataType: 'int32',
            buffer: intBuffer,
            dataView: new DataView(intBuffer),
            length: nRows  // Number of rows
        },
        {
            name: 'price',
            dataType: 'float32',
            buffer: floatBuffer,
            dataView: new DataView(floatBuffer),
            length: nRows  // Same number of rows for float32
        },
        {
            name: 'isAvailable',
            dataType: 'bool',
            buffer: boolBuffer,
            dataView: new DataView(boolBuffer),
            length: nRows  // Same number of rows for bools
        },
        {
            name: 'color',
            dataType: 'string',
            buffer: stringBuffer,
            dataView: new DataView(stringBuffer),
            length: nRows,  // Number of rows
            strings: stringValues  // String values for the string column
        }
    ];

    // Create a zero-copy DataFrame with 1 GB data buffers
    const df = new ZeroCopyDataFrame(columns);

    // Log system and DataFrame memory usage (optional)
    prettyPrintMemoryUsage({
        nRows,
    });

    // Access and print a few rows (avoid printing all rows since 1 GB has too many rows)
    console.log("First 5 rows:");
    for (let i = 0; i < 5; i++) {
        console.log(df.getRow(i));
    }

    console.time('Groupby SKU and Sum price');
    const groupedByInt = df.groupby('SKU');

    const summedFloatsByInt = df.sum(groupedByInt, 'price');
    console.log('Sum of price by SKU groups:', summedFloatsByInt);
    console.timeEnd('Groupby SKU and Sum price');

    prettyPrintMemoryUsage({
        nRows
    });

    console.time('Groupby color and Sum price');
    const groupedByColor = df.groupby('color');

    const summedFloatsByColor = df.sum(groupedByColor, 'price');
    console.log('Sum of price by color groups:', summedFloatsByColor);
    console.timeEnd('Groupby color and Sum price');

    prettyPrintMemoryUsage({
        nRows,
        df
    });

    exportToCSV(df, `outputs/test_${nRows}_rows.csv`);
}

main();