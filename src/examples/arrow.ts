import fs from 'fs';
import csvParser from 'csv-parser';
import {
    Table,
    Field,
    // Int32Vector,
    // Utf8Vector,
    Int32,
    Utf8,
    Vector,
    RecordBatch,
    Schema,
    makeVector
} from '@apache-arrow/ts';
import {
    prettyPrintMemoryUsage
} from '../utils';

function main() {
    const nRows = 100_000_000;
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
    const int8View = new DataView(intBuffer);
    const priceFloat32View = new DataView(floatBuffer);
    const boolView = new DataView(boolBuffer);
    const stringView = new DataView(stringBuffer);

    
    // Stream CSV and collect data
    const filePath = 'outputs/test_100000000_rows.csv'; // Replace with your actual file path
    const readStream = fs.createReadStream(filePath);

    let rowIndex = 0;
    
    readStream
        .pipe(csvParser())
        .on('data', (row: any) => {
            priceFloat32View.setFloat32(rowIndex * 4, row.price, true);
            rowIndex++;
        })
        .on('end', () => {
            const priceVector = makeVector(priceFloat32View);
            // print first 5 rows
            console.log(priceVector.slice(0, 5));
            // const column2Vector = new Vector({ type: new Utf8(), values: column2Data });
    
            // // Define the schema
            // const schema = new Schema([
            //     new Field('column1', new Int32()),
            //     new Field('column2', new Utf8()),
            // ]);
    
            // // Create a RecordBatch
            // const recordBatch = new RecordBatch(schema, [column1Vector, column2Vector]);
    
            // // Create a Table from the RecordBatch
            // const arrowTable = new Table(recordBatch);
    
            // // Log the Arrow Table
            // console.log(arrowTable.toString());
        })
        .on('error', (err: Error) => {
            console.error('Error reading CSV:', err);
        });
}

main();
