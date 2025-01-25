import { BaseDuckDBHandler } from "./base";
import { DuckDBConfig } from "./interface";

export class DuckDbExchangeRateHandler extends BaseDuckDBHandler {
  constructor(config: DuckDBConfig) {
    super(config);
  }

  public getDefaultQuery(): string {
    return `
      SELECT
        date,
        rate
      FROM exchange_rates
      ORDER BY date;
    `;
  }

  public async register(): Promise<void> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }

    const conn = await this.db.connect();

    try {
      const checkTable = await conn.query(`
        SELECT EXISTS (
          SELECT 1 
          FROM information_schema.tables 
          WHERE table_name = 'exchange_rates'
        ) as exists_flag;
      `);
      const existsFlag = checkTable.toArray()[0].exists_flag;

      if (!existsFlag) {
        await conn.query(`
          CREATE TABLE exchange_rates AS
          SELECT
            date_str::DATE AS date,
            rate.jpy AS rate
          FROM (
            SELECT
              unnest(map_keys(rates)) AS date_str,
              unnest(map_values(rates)) AS rate
            FROM
              read_json_auto('https://api.frankfurter.dev/v1/2023-01-01..2024-12-31?from=USD&to=JPY')
          );
        `);
      }
    } catch (error) {
      console.error("為替テーブル作成時エラー:", error);
    } finally {
      await conn.close();
    }
  }

  public async getRecordCount(): Promise<number> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    const conn = await this.db.connect();

    try {
      const res = await conn.query(`
        SELECT COUNT(*) AS cnt FROM exchange_rates;
      `);
      const rows = res.toArray();
      if (rows.length === 0) {
        return 0;
      }
      const rowObj = JSON.parse(rows[0]);
      return rowObj.cnt ?? 0;
    } finally {
      await conn.close();
    }
  }

  public async executeQuery(query: string): Promise<{
    headers: string[];
    rows: Record<string, any>[];
  }> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }

    const conn = await this.db.connect();
    try {
      const result = await conn.query(query);
      const rows = result.toArray();

      if (rows.length === 0) {
        return { headers: [], rows: [] };
      }

      const firstParsed = JSON.parse(rows[0]);
      const headers = Object.keys(firstParsed);
      const parsedRows = rows.map((r) => JSON.parse(r));

      return { headers, rows: parsedRows };
    } catch (error) {
      console.error("Query 実行エラー:", error);
      return { headers: [], rows: [] };
    } finally {
      await conn.close();
    }
  }

  public async search(
    searchTerm: string,
  ): Promise<{ headers: string[]; rows: Record<string, any>[] }> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }

    if (!searchTerm.trim()) {
      return { headers: [], rows: [] };
    }

    const query = `
      SELECT date, rate
      FROM exchange_rates
      WHERE CAST(date AS TEXT) LIKE '%${searchTerm}%'
      ORDER BY date;
    `;
    return this.executeQuery(query);
  }

  public async downloadSampleParquet(): Promise<Uint8Array | null> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    try {
      const conn = await this.db.connect();

      const parquetName =
        this.config.sampleParquetFileName || "exrate_sample.parquet";
      await conn.query(`
        COPY (SELECT * FROM exchange_rates) 
          TO '${parquetName}' (FORMAT 'parquet', COMPRESSION 'zstd');
      `);
      const parquetBuffer = await this.db.copyFileToBuffer(parquetName);
      return parquetBuffer;
    } catch (error) {
      console.error("Parquet ダウンロード時エラー:", error);
      return null;
    }
  }
}
