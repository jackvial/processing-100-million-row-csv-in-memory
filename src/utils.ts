import { ZeroCopyDataFrame, Column, DataType } from './df';
import { StringColumnDict } from './types';

export function prettyPrintMemoryUsage({
    nRows,
    df
}: {
    nRows?: number,
    df?: ZeroCopyDataFrame
}) {
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
