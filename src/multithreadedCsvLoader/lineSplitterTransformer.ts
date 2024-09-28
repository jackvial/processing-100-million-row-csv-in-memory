// LineSplitter.ts
import { Transform, TransformCallback } from 'stream';

class LineSplitter extends Transform {
  private bufferedData: Buffer | null;

  constructor(options = {}) {
    super({ ...options, readableObjectMode: true });
    this.bufferedData = null;
  }

  _transform(chunk: Buffer, encoding: string, callback: TransformCallback) {
    let data: Buffer;

    if (this.bufferedData) {
      data = Buffer.concat([this.bufferedData, chunk]);
    } else {
      data = chunk;
    }

    let start = 0;
    let index: number;

    while ((index = data.indexOf(10, start)) !== -1) { // 10 is '\n'
      const line = data.subarray(start, index); // Exclude the newline character
      this.push(line);
      start = index + 1;
    }

    if (start < data.length) {
      this.bufferedData = data.subarray(start);
    } else {
      this.bufferedData = null;
    }

    callback();
  }

  _flush(callback: TransformCallback) {
    if (this.bufferedData) {
      this.push(this.bufferedData);
      this.bufferedData = null;
    }
    callback();
  }
}

export default LineSplitter;
