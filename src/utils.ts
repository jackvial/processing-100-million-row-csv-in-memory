import { ZeroCopyDataFrame, Column, DataType } from './df';
import { StringColumnDict } from './types';
import fs from 'fs';

export type MemoryStatsRow = {
    rows: number,
    rss: number,
    heapTotal: number,
    heapUsed: number,
    external: number,
    arrayBuffers: number,
    timestamp: number,
};

export function getMemoryStats(
    nRows: number,
    df?: ZeroCopyDataFrame
): MemoryStatsRow {
    console.log('----------------------------------------');
    console.log(`Memory Usage with ${nRows} rows:`);
    const memoryUsage = process.memoryUsage();

    console.log('Memory Usage:');
    console.log(`RSS: ${bytesToMB(memoryUsage.rss)} MB`);
    console.log(`Heap Total: ${bytesToMB(memoryUsage.heapTotal)} MB`);
    console.log(`Heap Used: ${bytesToMB(memoryUsage.heapUsed)} MB`);
    console.log(`External: ${bytesToMB(memoryUsage.external)} MB`);
    console.log(`Array Buffers: ${bytesToMB(memoryUsage.arrayBuffers)} MB`);

    if (df) {
        const totalAllocatedMemory = df.columns.reduce((sum, column) => sum + column.buffer.byteLength, 0);
        console.log(`DataFrame Memory Allocation: ${bytesToMB(totalAllocatedMemory)} MB`);
    }

    return {
        rows: nRows,
        rss: memoryUsage.rss,
        heapTotal: memoryUsage.heapTotal,
        heapUsed: memoryUsage.heapUsed,
        external: memoryUsage.external,
        arrayBuffers: memoryUsage.arrayBuffers,
        timestamp: Date.now(),
    };
}

export function bytesToMB(bytes: number): string {
    return (bytes / 1024 / 1024).toFixed(4);
}

export function finalizeDataFrame(columns: Column[]): ZeroCopyDataFrame {
    return new ZeroCopyDataFrame(columns);
}

export function getColumnSize(dataType: DataType): number {
    switch (dataType) {
        case 'int32': return 4;
        case 'float32': return 4;
        case 'bool': return 1;
        case 'string': return 1;  // Index for strings
        default: throw new Error(`Unsupported data type: ${dataType}`);
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
                // if (isNaN(value)) {
                //     throw new Error(`Invalid string value: ${value}`);
                // }
                if (!(value in dict.valueToIndex)) {
                    dict.valueToIndex[value] = dict.strings.length;
                    dict.strings.push(value);
                }
                col.dataView.setUint8(offset, dict.valueToIndex[value]);  // Store the index of the string
                col.strings = dict.strings;  // Update the string array
                break;
            default:
                throw new Error(`Unsupported data type: ${col.dataType}`);
        }
    });
}

// Helper function to convert an array of MemoryStatsRow to CSV format
function memoryStatsToCSV(memoryStatsArray: MemoryStatsRow[]): string {
    // CSV header
    const header = 'rows,rss,heapTotal,heapUsed,external,arrayBuffers,timestamp\n';

    // Map each row to a CSV string
    const rows = memoryStatsArray.map(stat => {
        return `${stat.rows},${stat.rss},${stat.heapTotal},${stat.heapUsed},${stat.external},${stat.arrayBuffers},${stat.timestamp}`;
    });

    // Combine header and rows
    return header + rows.join('\n');
}

// Function to write memory stats array to CSV file
export function writeStatsToCsv(
    {
        memoryStats,
        duration
    }: {
        memoryStats: MemoryStatsRow[],
        duration: number
    },
    filePath: string,
): void {
    // Convert memory stats array to CSV
    const csvData = memoryStatsToCSV(memoryStats);

    // Write the CSV data to the specified file
    fs.writeFileSync(filePath, csvData, 'utf8');
    console.log(`Memory stats written to ${filePath}`);

    // Write the duration to a separate file
    fs.writeFileSync(filePath.replace('.csv', '_duration.txt'), duration.toString(), 'utf8');
}
