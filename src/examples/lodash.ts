import _ from 'lodash';
import {
    getMemoryStats
} from '../utils';

function main() {
    // loadash groupBy and sum over dataset of 1 million rows
    const nRows = 10_000_000;
    const data = _.times(nRows, (i) => ({
        SKU: i % 100,
        price: Math.random() * 100,
        isAvailable: Math.random() > 0.5,
        color: ['red', 'blue', 'green', 'yellow'][i % 4]
    }));

    const summed = _.groupBy(data, 'color').sumBy('price')
    console.log(summed);

    console.log(getMemoryStats(nRows));

}

main();