
import fs from 'fs';
import { ZeroCopyDataFrame, DataType, Column, getColumnSize } from './df';

// Function to export DataFrame to CSV
export function exportToCSV(df: ZeroCopyDataFrame, outputFilePath: string) {
    const writeStream = fs.createWriteStream(outputFilePath);

    // Write the header row with column names
    const headers = df.columns.map((col) => col.name).join(',');
    writeStream.write(headers + '\n');

    // Write each row of data
    for (let i = 0; i < df.columns[0].length; i++) {
        const row = df.getRow(i);
        const formattedRow = df.columns.map((col) => formatValueForCSV(row[col.name], col.dataType)).join(',');
        writeStream.write(formattedRow + '\n');
    }

    writeStream.end();
    console.log(`DataFrame successfully exported to ${outputFilePath}`);
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

// Allocate buffers based on row count and data types
export function allocateBuffers(rowCount: number, columns: { name: string; dataType: DataType; }[]) {
    return columns.map((col) => {
        let bufferSize: number;
        switch (col.dataType) {
            case 'int32': bufferSize = rowCount * 4; break;
            case 'float32': bufferSize = rowCount * 4; break;
            case 'bool': bufferSize = rowCount * 1; break;
            case 'string': bufferSize = rowCount * 1; break;  // 1 byte per string index
            default: throw new Error(`Unsupported data type: ${col.dataType}`);
        }
        return {
            name: col.name,
            dataType: col.dataType,
            buffer: new ArrayBuffer(bufferSize),
            dataView: new DataView(new ArrayBuffer(bufferSize)),
            length: rowCount,
        };
    });
}

// Define an interface for the string dictionary
export interface StringColumnDict {
    valueToIndex: { [value: string]: number };  // Mapping from string to index
    strings: string[];  // Array of unique strings
}