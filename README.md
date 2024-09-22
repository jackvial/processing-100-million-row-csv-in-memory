# Processing 100 Million Record CSV In Nodejs

Assume we need the whole file in memory to perform multiple operations like:
- Sorting
- Join multiple files together (like SQL inner join)
- Complex Grouping or Aggregation Across Multiple Columns
- Deduplication
- Window functions

That would be really inconvenient or slow to do otherwise.
Interesting problems to think about if you did not have the choice of loading all data in memory.

1. Parsing the csv. Using CSV Parser.
    - Use WasteManifest or EchoExporter in example. Has a lot of columns
2. Storing the data in memory. Naive memory implementation with array of objects. Memory usage grows quickly and runs out of memory
4. Basic ArrayBuffer example
    - Run first then talk about:
        - Available data types
        - Populating buffer, data views, zero-copy
        - How to store strings using a dictionary and index
        - Handling null values
5. Why is ArrayBuffer so much more memory efficient than Array of objects?
6. Grouping and summing the data example
7. Drop duplicates example, need more data in memory
8. Wouldn't it be nice if there was a library that done all of this. Introduce Simple Columnar DataFrame Library. Why not use lodash?
9. Introduce Apache Arrow Columnar Data Format
    - https://arrow.apache.org/docs/format/Columnar.html
10. Apache Arrow JS
    - Very bare bones. Only provides the Apache Arrow primitives
    - Can't use because of bugs and JS/TS project is not well maintained. Reported bug here https://github.com/apache/arrow/issues/44180
11. Polars JS
    - Fast
    - Was able to get working after fixing bugs. Opened PR here https://github.com/pola-rs/nodejs-polars/pull/275
    - Not well use or battle tested. Need to be willing to contribute to the Github
12. Pandas
    - TODO
13. PyArrow
    - TODO
14. Polars Python
    - TODO
15. Conclusion
    - ArrayBuffer is definitely a valid option, especially for dealing with number types
    - Need to do a bit more work with string types
    - Keeping my eye on Polar JS
    - Python ecosystem is more mature, community much more active, and ChatGPT can help you a lot here

## Notes
- Use charts and graph or just run and talk about examples in VSCode?


## Datasets
- eManifest WasteLines https://s3.amazonaws.com/rcrainfo-ftp/Production/Fixed-2024-09-16T03-00-00-0400/eManifest/EM_WASTE_LINE/EM_WASTE_LINE.zip