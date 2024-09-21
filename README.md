# Processing A 10 Million Row CSV In Nodejs

1. Parsing the csv. Using CSV Parser.
    - Use WasteManifest or EchoExporter in example. Has a lot of columns
2. Storing the data in memory. Naive memory implementation with array of objects. Memory usage grows quickly and runs out of memory
3. Introduce Apache Arrow Columnar Data Format
    - https://arrow.apache.org/docs/format/Columnar.html
4. Basic ArrayBuffer example
    - Run first then talk about:
        - Available data types
        - Populating buffer, data views, zero-copy
        - How to store strings using a dictionary and index
5. Grouping and summing the data example
6. Drop duplicates example, need more data in memory
7. Wouldn't it be nice if there was a library that done all of this. Introduce Simple Columnar DataFrame Library
8. ArrowJS. PolarsJS.
9. Python Ecosystem

## Notes
- Use charts and graph or just run and talk about examples in VSCode?
