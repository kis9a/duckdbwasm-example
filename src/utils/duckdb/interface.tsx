export interface IDuckDBHandler {
  init(): Promise<void>;
  getVersion(): Promise<string>;
  getDuckDBWasmVersion(): string;
  getDefaultQuery(): string;
  register(): Promise<void>;
  getRecordCount(): Promise<number>;
  executeQuery(
    query: string,
  ): Promise<{ headers: string[]; rows: Record<string, any>[] }>;
  search(
    searchTerm: string,
  ): Promise<{ headers: string[]; rows: Record<string, any>[] }>;
  downloadSampleParquet(): Promise<Uint8Array | null>;
  purge(): Promise<void>;
}

export interface DuckDBConfig {
  dbPath: string;
  parquetFileName: string;
  sampleParquetFileName: string;
  parquetSourceUrl: string;
}
