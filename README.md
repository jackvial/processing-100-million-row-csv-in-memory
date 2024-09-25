# Processing 100 Million Record CSV In Nodejs

## Creating The Data
### Python Virtual Environment Setup
- `python3 -m venv one_hundred_million_env`
- `source one_hundred_million_env/bin/activate`
- `pip install -r requirements.txt`

### Generate Data
`python py/src/createChemicalsShippedDataset.py`
- This will create a file `outputs/chemicals_shipped_100000000.csv` with 100 million records.

## Running The NodeJS Examples
- `npm install`
- `npx tsc`
- `node dist/examples/arrayBufferBetterTypes.js`

## Running The Python Examples
- Create python virtual environment as described above if you haven't done so already.
- `python py/src/examples/example_pandas.py`
