import pl from 'nodejs-polars';
import {
    prettyPrintMemoryUsage
} from '../utils';

async function main() {
    const nRows = 100_000_000;
    const df = pl.readCSV('outputs/test_100000000_rows.csv');

    prettyPrintMemoryUsage({
        nRows
    });

    // Print head of the DataFrame
    console.log(df.head());

    prettyPrintMemoryUsage({
        nRows
    });

    // Group by color and sum price
    const out = df.groupBy('color').agg({
        price: ['sum']
    });

    // Print out the result
    console.log(out);
}

main();