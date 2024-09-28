import fs from 'fs';
import os from 'os';
import { Worker, isMainThread, parentPort, workerData, WorkerOptions } from 'worker_threads';
import readline from 'readline';
import { getMemoryStats } from "../utils";

interface CSVReaderOptions {
    separator?: string;
    hasHeader?: boolean;
    schema?: Array<{ name: string; type: string }>;
    projection?: number[];
    chunkSize?: number;
    encoding?: BufferEncoding;
    commentPrefix?: string;
    quoteChar?: string;
    ignoreErrors?: boolean;
    numWorkers?: number;
}

interface WorkerData {
    filePath: string;
    start: number;
    end: number;
    options: CSVReaderOptions;
    sharedBuffer: SharedArrayBuffer;
    bufferOffset: number;
}

class FastCSVReader {
    private separator: string;
    private hasHeader: boolean;
    private schema: Array<{ name: string; type: string }> | null;
    private projection: number[] | null;
    private chunkSize: number;
    private encoding: BufferEncoding;
    private commentPrefix: string | null;
    private quoteChar: string;
    private ignoreErrors: boolean;
    private numWorkers: number;
    private sharedBuffer: SharedArrayBuffer;
    private sharedView: Int32Array;

    constructor(options: CSVReaderOptions = {}) {
        this.separator = options.separator || ',';
        this.hasHeader = options.hasHeader !== undefined ? options.hasHeader : true;
        this.schema = options.schema || null;
        this.projection = options.projection || null;
        this.chunkSize = options.chunkSize || 10000;
        this.encoding = options.encoding || 'utf8';
        this.commentPrefix = options.commentPrefix || null;
        this.quoteChar = options.quoteChar || '"';
        this.ignoreErrors = options.ignoreErrors || false;
        this.numWorkers = options.numWorkers || os.cpus().length;
        
        // Initialize SharedArrayBuffer
        this.sharedBuffer = new SharedArrayBuffer(this.numWorkers * 4); // 4 bytes per worker
        this.sharedView = new Int32Array(this.sharedBuffer);
    }

    async *readFile(filePath: string): AsyncGenerator<object[]> {
        const fileSize = fs.statSync(filePath).size;
        const chunkSize = Math.floor(fileSize / this.numWorkers);
        const workers: Worker[] = [];

        for (let i = 0; i < this.numWorkers; i++) {
            const start = i * chunkSize;
            const end = i === this.numWorkers - 1 ? fileSize : (i + 1) * chunkSize;
            workers.push(this.createWorker(filePath, start, end, i * 4));
        }

        const headers = await this.getHeaders(filePath);

        const results = await Promise.all(workers.map(worker => {
            return new Promise<void>((resolve, reject) => {
                worker.on('message', resolve);
                worker.on('error', reject);
            });
        }));

        yield* this.mergeResults(headers);
    }

    private createWorker(filePath: string, start: number, end: number, bufferOffset: number): Worker {
        const workerOptions: WorkerOptions = {
            workerData: {
                filePath,
                start,
                end,
                options: {
                    separator: this.separator,
                    hasHeader: this.hasHeader,
                    projection: this.projection,
                    chunkSize: this.chunkSize,
                    commentPrefix: this.commentPrefix,
                    quoteChar: this.quoteChar,
                    ignoreErrors: this.ignoreErrors,
                    encoding: this.encoding,
                },
                sharedBuffer: this.sharedBuffer,
                bufferOffset,
            },
        };
        return new Worker(__filename, workerOptions);
    }

    private async getHeaders(filePath: string): Promise<string[]> {
        if (!this.hasHeader) return [];

        const fileStream = fs.createReadStream(filePath, { encoding: this.encoding });
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity,
        });

        for await (const line of rl) {
            rl.close();
            fileStream.close();
            return this.parseLine(line);
        }

        return [];
    }

    private *mergeResults(headers: string[]): Generator<object[]> {
        let totalProcessed = 0;
        while (totalProcessed < this.sharedView.length) {
            const chunk: object[] = [];
            for (let i = 0; i < this.numWorkers; i++) {
                const processed = Atomics.load(this.sharedView, i);
                if (processed > 0) {
                    const workerChunk = this.readWorkerChunk(i, processed);
                    chunk.push(...this.processChunk(headers, workerChunk));
                    Atomics.store(this.sharedView, i, 0);
                    totalProcessed++;
                }
            }
            if (chunk.length > 0) {
                yield chunk;
            }
        }
    }

    private readWorkerChunk(workerId: number, processed: number): string[][] {
        // Read the chunk from the file or another shared resource
        // This is a placeholder and needs to be implemented based on how you store the actual data
        return [];
    }

    private parseLine(line: string): string[] {
        return line.split(this.separator).map(field => field.trim().replace(/^"|"$/g, ''));
    }

    private processChunk(headers: string[], data: string[][]): object[] {
        return data.map(row => {
            const obj: { [key: string]: string } = {};
            headers.forEach((header, index) => {
                obj[header] = row[index];
            });
            return obj;
        });
    }

    static async processChunk(filePath: string, start: number, end: number, options: CSVReaderOptions, sharedView: Int32Array, bufferOffset: number): Promise<void> {
        const fileStream = fs.createReadStream(filePath, {
            start,
            end: end - 1,
            encoding: options.encoding
        });
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity,
        });

        let processedRows = 0;
        for await (const line of rl) {
            if (!options.commentPrefix || !line.startsWith(options.commentPrefix)) {
                const parsedLine = FastCSVReader.parseLine(line, options.separator!);
                // Store the parsed line in a shared resource or file
                // This is a placeholder and needs to be implemented
                processedRows++;
                if (processedRows % options.chunkSize! === 0) {
                    Atomics.add(sharedView, bufferOffset / 4, processedRows);
                    processedRows = 0;
                }
            }
        }

        if (processedRows > 0) {
            Atomics.add(sharedView, bufferOffset / 4, processedRows);
        }
    }

    private static parseLine(line: string, separator: string): string[] {
        return line.split(separator).map(field => field.trim().replace(/^"|"$/g, ''));
    }
}

if (!isMainThread) {
    const { filePath, start, end, options, sharedBuffer, bufferOffset } = workerData as WorkerData;
    const sharedView = new Int32Array(sharedBuffer);
    FastCSVReader.processChunk(filePath, start, end, options, sharedView, bufferOffset).then(() => {
        parentPort!.postMessage('done');
    });
}

async function main() {
    const reader = new FastCSVReader({ hasHeader: true, chunkSize: 5000, numWorkers: 4 });
    let nRows = 0;
    for await (const chunk of reader.readFile('outputs/chemicals_shipped_100000000.csv')) {
        nRows += chunk.length;
        if (nRows % 1_000_000 === 0) {
            console.log(`Rows: ${nRows}`);
            console.log(getMemoryStats(nRows));
        }
    }
}

main();