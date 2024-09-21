# Processing A 10 Million Row CSV In Nodejs

1. Parsing the csv. Using CSV Parser.
    - Use WasteManifest or EchoExporter in example. Has a lot of columns
2. Storing the data in memory. Naive memory implementation with array of objects. Memory usage grows quickly and runs out of memory
3. Basic ArrayBuffer example
    - Run first then talk about:
        - Available data types
        - Populating buffer, data views, zero-copy
        - How to store strings using a dictionary and index
4. Grouping and summing the data example
5. Simple Columnar DataFrame Library
6. ArrowJS. PolarsJS.
7. Python Ecosystem
