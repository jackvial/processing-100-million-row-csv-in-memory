
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

// Define the string column dictionaries for each string column
const stringColumnDicts: { [key: string]: StringColumnDict } = {
    color_col: {
        valueToIndex: { red: 0, green: 1, blue: 2, purple: 3 },  // Predefined mapping
        strings: ['red', 'green', 'blue', 'purple'],  // Predefined list of unique strings
    }
};


// Populate buffers for each row
export function populateBuffersFromRow(
    rowIndex: number,
    rowData: any,
    columns: Column[],
    stringColumnDicts: { [key: string]: StringColumnDict }  // Now using the defined interface
) {
    columns.forEach((col) => {
        const value = rowData[col.name];
        const offset = rowIndex * getColumnSize(col.dataType);

        switch (col.dataType) {
            case 'int32':
                col.dataView.setInt32(offset, parseInt(value), true);
                break;
            case 'float32':
                col.dataView.setFloat32(offset, parseFloat(value), true);
                break;
            case 'bool':
                col.dataView.setUint8(offset, value === 'true' ? 1 : 0);
                break;
            case 'string':
                const dict = stringColumnDicts[col.name];
                if (!(value in dict.valueToIndex)) {
                    dict.valueToIndex[value] = dict.strings.length;
                    dict.strings.push(value);
                }
                col.dataView.setUint8(offset, dict.valueToIndex[value]);  // Store the index of the string
                break;
            default:
                throw new Error(`Unsupported data type: ${col.dataType}`);
        }
    });
}
