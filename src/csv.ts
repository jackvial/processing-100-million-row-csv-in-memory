import fs from 'fs';
import { ZeroCopyDataFrame, DataType } from './df';
import {
    prettyPrintMemoryUsage
} from "./utils";

// Function to export DataFrame to CSV in a memory-efficient way
export function exportToCSV(df: ZeroCopyDataFrame, outputFilePath: string, batchSize: number = 10000) {
    const writeStream = fs.createWriteStream(outputFilePath);
    
    // Write the header row with column names
    const headers = df.columns.map((col) => col.name).join(',');
    writeStream.write(headers + '\n');

    let rowsProcessed = 0;

    // Function to process and write a batch of rows
    // Writing in batches like this will prevent running out of memory
    // because memory will be freed when this function returns
    function writeBatch(startIndex: number, batchSize: number) {
        let buffer = '';

        for (let i = startIndex; i < Math.min(startIndex + batchSize, df.columns[0].length); i++) {
            if (i % 1000000 === 0) {
                prettyPrintMemoryUsage({
                    nRows: i,
                    df
                });
            }

            const row = df.getRow(i);
            const formattedRow = df.columns.map((col) => formatValueForCSV(row[col.name], col.dataType)).join(',');
            buffer += formattedRow + '\n';
        }

        return new Promise<void>((resolve) => {
            writeStream.write(buffer, () => resolve());
        });
    }

    // Process and write rows in batches
    async function writeInBatches() {
        for (let i = 0; i < df.columns[0].length; i += batchSize) {
            await writeBatch(i, batchSize);  // Write each batch and wait for it to finish

            // Manually trigger garbage collection after each batch (only in Node.js with --expose-gc)
            if (global.gc) {
                global.gc();
            }

            rowsProcessed += batchSize;
        }

        writeStream.end();
        console.log(`DataFrame successfully exported to ${outputFilePath}. Total rows: ${rowsProcessed}`);
    }

    writeInBatches().catch((err) => {
        console.error("Error writing CSV:", err);
        writeStream.end();
    });
}

// Helper function to format values for CSV
export function formatValueForCSV(value: any, dataType: DataType): string {
    switch (dataType) {
        case 'string':
            return `"${value}"`;  // Enclose strings in quotes
        case 'bool':
            return value ? 'true' : 'false';  // Convert boolean to 'true'/'false'
        default:
            return String(value);  // Convert other types to string
    }
}
