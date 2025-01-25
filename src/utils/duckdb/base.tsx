import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?worker";
import duckdbWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import { IDuckDBHandler, DuckDBConfig } from "./interface";

export abstract class BaseDuckDBHandler implements IDuckDBHandler {
  protected db: duckdb.AsyncDuckDB | null = null;
  protected duckdbWasmVersion: string = duckdb.PACKAGE_VERSION;
  constructor(protected config: DuckDBConfig) {}

  public async init(): Promise<void> {
    if (this.db) {
      return;
    }
    const worker = new duckdbWorker();
    const logger = new duckdb.ConsoleLogger();

    const _db = new duckdb.AsyncDuckDB(logger, worker);
    await _db.instantiate(duckdbWasm);

    await _db.open({
      path: this.config.dbPath,
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
    });

    const conn = await _db.connect();
    await conn.query(`
      INSTALL parquet;
      LOAD parquet;
      INSTALL json;
      LOAD json;
    `);
    await conn.close();

    this.db = _db;
  }

  public async getVersion(): Promise<string> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    return await this.db.getVersion();
  }

  public getDuckDBWasmVersion(): string {
    return this.duckdbWasmVersion;
  }

  public async purge(): Promise<void> {
    if (this.db) {
      await this.db.terminate();
      this.db = null;
    }

    try {
      const opfsRoot = await navigator.storage.getDirectory();
      await opfsRoot.removeEntry(this.config.dbPath.replace("opfs://", ""));
      await opfsRoot
        .removeEntry(this.config.dbPath.replace("opfs://", "") + ".wal")
        .catch(() => {});
    } catch (err) {
      console.warn("OPFS ファイル削除エラー:", err);
    }
  }

  public abstract register(): Promise<void>;
  public abstract getDefaultQuery(): string;
  public abstract getRecordCount(): Promise<number>;
  public abstract executeQuery(query: string): Promise<{
    headers: string[];
    rows: Record<string, any>[];
  }>;
  public abstract search(
    searchTerm: string,
  ): Promise<{ headers: string[]; rows: Record<string, any>[] }>;
  public abstract downloadSampleParquet(): Promise<Uint8Array | null>;
}
