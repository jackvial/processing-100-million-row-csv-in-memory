type DataType = 'int32' | 'float32' | 'bool' | 'string';

interface Column {
  name: string;
  dataType: DataType;
  buffer: ArrayBuffer;
  dataView: DataView;
  length: number;
  strings?: string[];
}

class ZeroCopyDataFrame {
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
    switch (dataType) {
      case 'int32':
        return 4;
      case 'float32':
        return 4;
      case 'bool':
        return 1;
    case 'string':
        return 1;  // 1 byte to store the index of the string
      default:
        throw new Error(`Unsupported data type: ${dataType}`);
    }
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

  // Method to print the dataframe
  print(): void {
    for (let i = 0; i < this.columns[0].length; i++) {
      console.log(this.getRow(i));
    }
  }
}

function bytesToMB(bytes: number): string {
    return (bytes / 1024 / 1024).toFixed(4);
}

function prettyPrintMemoryUsage({
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


function main() {
    // Example: Creating a DataFrame

    const nRows = 500_000_000;
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
      name: 'int_col',
      dataType: 'int32',
      buffer: intBuffer,
      dataView: new DataView(intBuffer),
      length: nRows  // Number of rows
    },
    {
      name: 'float_col',
      dataType: 'float32',
      buffer: floatBuffer,
      dataView: new DataView(floatBuffer),
      length: nRows  // Same number of rows for float32
    },
    {
      name: 'bool_col',
      dataType: 'bool',
      buffer: boolBuffer,
      dataView: new DataView(boolBuffer),
      length: nRows  // Same number of rows for bools
    },
    {
      name: 'color_col',
      dataType: 'string',
      buffer: stringBuffer,
      dataView: new DataView(stringBuffer),
      length: nRows,  // Number of rows
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
    
    // Perform groupby on 'int_col' and sum on 'float_col'
    const groupedByInt = df.groupby('int_col');
    
    const summedFloatsByInt = df.sum(groupedByInt, 'float_col');
    console.log('Sum of float_col by int_col groups:', summedFloatsByInt);

    prettyPrintMemoryUsage({
        nRows
    });

    // Group by 'color_col' and sum 'float_col'
    const groupedByColor = df.groupby('color_col');

    const summedFloatsByColor = df.sum(groupedByColor, 'float_col');
    console.log('Sum of float_col by color_col groups:', summedFloatsByColor);

    prettyPrintMemoryUsage({
        nRows,
        df
    });
}

main();
