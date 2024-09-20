// Define an interface for the string dictionary
export interface StringColumnDict {
    valueToIndex: { [value: string]: number };  // Mapping from string to index
    strings: string[];  // Array of unique strings
}
