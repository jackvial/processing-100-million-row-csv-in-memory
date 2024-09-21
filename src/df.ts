import { getColumnSize } from './utils';

export type DataType = 'int32' | 'float32' | 'bool' | 'string';

export interface Column {
    name: string;
    dataType: DataType;
    buffer: ArrayBuffer;
    dataView: DataView;
    length: number;
    strings?: string[];
}

export class ZeroCopyDataFrame {
    columns: Column[];

    constructor(columns: Column[]) {
        this.columns = columns;
    }

    // Get a row by index
    getRow(index: number): any {
        const row: any = {};
        for (const column of this.columns) {
            row[column.name] = this.getColumnValue(column, index);
        }
        return row;
    }

    // Helper function to get a value from a column at a given row index
    getColumnValue(column: Column, index: number): any {
        const { dataType, dataView } = column;
        const offset = this.getOffset(column, index);

        switch (dataType) {
            case 'int32':
                return dataView.getInt32(offset, true);  // Little-endian
            case 'float32':
                return dataView.getFloat32(offset, true);
            case 'bool':
                return dataView.getUint8(offset) === 1;  // Boolean values as 0/1
            case 'string':
                if (!column.strings) throw new Error(`String column missing string array`);
                const stringIndex = dataView.getUint8(offset);
                return column.strings[stringIndex];
            default:
                throw new Error(`Unsupported data type: ${dataType}`);
        }
    }

    // Calculate byte offset for each row based on the type size
    getOffset(column: Column, index: number): number {
        const { dataType } = column;
        const typeSize = this.getTypeSize(dataType);
        return index * typeSize;
    }

    // Get size of data type in bytes
    getTypeSize(dataType: DataType): number {
        return getColumnSize(dataType);
    }

    // Groupby function that creates groups based on a column's values
    groupby(columnName: string): { [key: string]: number[] } {
        const column = this.columns.find(col => col.name === columnName);
        if (!column) {
            throw new Error(`Column ${columnName} not found`);
        }

        const groups: { [key: string]: number[] } = {};
        for (let i = 0; i < column.length; i++) {
            const value = String(this.getColumnValue(column, i));
            if (!groups[value]) {
                groups[value] = [];
            }
            groups[value].push(i);  // Store row indices
        }

        return groups;
    }

    // Sum function to sum values of a column based on groups
    sum(groupedBy: { [key: string]: number[] }, columnName: string): { [key: string]: number } {
        const column = this.columns.find(col => col.name === columnName);
        if (!column) {
            throw new Error(`Column ${columnName} not found`);
        }

        const sums: { [key: string]: number } = {};

        for (const [group, indices] of Object.entries(groupedBy)) {
            sums[group] = indices.reduce((sum, index) => {
                const value = this.getColumnValue(column, index);
                return sum + value;
            }, 0);
        }

        return sums;
    }

    // @TODO - add a dropDuplicates(columns: string[]) method
}