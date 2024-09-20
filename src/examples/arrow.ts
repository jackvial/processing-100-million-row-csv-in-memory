// import fs from 'fs';
// import csvParser from 'csv-parser';
// import {
//     Table,
//     Field,
//     // Int32Vector,
//     // Utf8Vector,
//     Int32,
//     Utf8,
//     Vector,
//     RecordBatch,
//     Schema,
// } from '@apache-arrow/ts';

// // Define types for CSV rows
// interface CSVRow {
//     column1: string;  // Assuming column1 is a number in string format
//     column2: string;  // Assuming column2 is a string
//     // Add other columns as needed
// }

// // Variables to store data before we create the Arrow table
// const column1Data: number[] = [];
// const column2Data: string[] = [];

// // Stream CSV and collect data
// const filePath = 'your-file.csv'; // Replace with your actual file path
// const readStream = fs.createReadStream(filePath);

// readStream
//     .pipe(csvParser())
//     .on('data', (row: CSVRow) => {
//         // Push parsed data into respective arrays
//         column1Data.push(parseInt(row.column1)); // Convert string to number
//         column2Data.push(row.column2);           // Keep as string
//     })
//     .on('end', () => {
//         // Create Apache Arrow vectors for each column
//         // const column1Vector = new Vector({ type: new Int32(), values: column1Data });
//         // const column2Vector = new Vector({ type: new Utf8(), values: column2Data });

//         // // Define the schema
//         // const schema = new Schema([
//         //     new Field('column1', new Int32()),
//         //     new Field('column2', new Utf8()),
//         // ]);

//         // // Create a RecordBatch
//         // const recordBatch = new RecordBatch(schema, [column1Vector, column2Vector]);

//         // // Create a Table from the RecordBatch
//         // const arrowTable = new Table(recordBatch);

//         // // Log the Arrow Table
//         // console.log(arrowTable.toString());
//     })
//     .on('error', (err: Error) => {
//         console.error('Error reading CSV:', err);
//     });