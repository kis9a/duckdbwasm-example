import { DuckDBConfig } from "./interface";
import { BaseDuckDBHandler } from "./base";

export class DuckDbRtcLogHandler extends BaseDuckDBHandler {
  constructor(config: DuckDBConfig) {
    if (!config.dbPath) {
      throw new Error("dbPath is required");
    }
    if (!config.parquetFileName) {
      throw new Error("parquetFileName is required");
    }
    if (!config.sampleParquetFileName) {
      throw new Error("sampleParquetFileName is required");
    }
    if (!config.parquetSourceUrl) {
      throw new Error("parquetSourceUrl is required");
    }
    super(config);
  }

  public async getVersion(): Promise<string> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    const version = await this.db.getVersion();
    return version;
  }

  public getDefaultQuery(): string {
    return `SELECT
    time_bucket,
    channel_id,
    session_id,
    connection_id,
    bytes_sent_diff,
    bytes_received_diff,
    packets_sent_diff,
    packets_received_diff
  FROM (
    SELECT
      time_bucket,
      channel_id,
      session_id,
      connection_id,
      bytes_sent - LAG(bytes_sent) OVER (PARTITION BY channel_id, session_id, connection_id ORDER BY time_bucket) AS bytes_sent_diff,
      bytes_received - LAG(bytes_received) OVER (PARTITION BY channel_id, session_id, connection_id ORDER BY time_bucket) AS bytes_received_diff,
      packets_sent - LAG(packets_sent) OVER (PARTITION BY channel_id, session_id, connection_id ORDER BY time_bucket) AS packets_sent_diff,
      packets_received - LAG(packets_received) OVER (PARTITION BY channel_id, session_id, connection_id ORDER BY time_bucket) AS packets_received_diff
    FROM (
      SELECT
        strftime(time_bucket('15 seconds', strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')), '%Y-%m-%d %H:%M:%S') AS time_bucket,
        channel_id,
        session_id,
        connection_id,
        MAX(CAST(rtc_data->'$.bytesSent' AS BIGINT)) AS bytes_sent,
        MAX(CAST(rtc_data->'$.bytesReceived' AS BIGINT)) AS bytes_received,
        MAX(CAST(rtc_data->'$.packetsSent' AS BIGINT)) AS packets_sent,
        MAX(CAST(rtc_data->'$.packetsReceived' AS BIGINT)) AS packets_received
      FROM rtc_stats
      WHERE rtc_type = 'transport'
      GROUP BY time_bucket, channel_id, session_id, connection_id
    )
  ) 
  WHERE 
    bytes_sent_diff IS NOT NULL AND
    bytes_received_diff IS NOT NULL AND
    packets_sent_diff IS NOT NULL AND
    packets_received_diff IS NOT NULL
  ORDER BY time_bucket ASC;`;
  }

  public async register(): Promise<void> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    try {
      const resp = await fetch(this.config.parquetSourceUrl);
      const buffer = await resp.arrayBuffer();

      await this.db.registerFileBuffer(
        this.config.parquetFileName,
        new Uint8Array(buffer),
      );

      const conn = await this.db.connect();
      const tableExists = await conn.query(`
        SELECT EXISTS (
          SELECT 1 
          FROM information_schema.tables 
          WHERE table_name = 'rtc_stats'
        ) as exists_flag;
      `);
      const existsFlag = tableExists.toArray()[0].exists_flag;

      if (!existsFlag) {
        await conn.query(`
          CREATE TABLE rtc_stats AS
          SELECT * FROM read_parquet('${this.config.parquetFileName}');
        `);
      }
      await conn.close();
    } catch (error) {
      console.error("Parquet ファイルの読み込みエラー:", error);
    }
  }

  public async getRecordCount(): Promise<number> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    const conn = await this.db.connect();
    const res = await conn.query(`
      SELECT COUNT(*) AS cnt FROM rtc_stats;
    `);
    const rows = res.toArray();
    await conn.close();

    if (rows.length === 0) {
      return 0;
    }
    const rowObj = JSON.parse(rows[0]);
    return rowObj.cnt ?? 0;
  }

  public async executeQuery(query: string): Promise<{
    headers: string[];
    rows: Record<string, any>[];
  }> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    try {
      const conn = await this.db.connect();
      const result = await conn.query(query);
      const rows = result.toArray();

      await conn.close();

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
      SELECT timestamp, connection_id, rtc_type
      FROM rtc_stats
      WHERE connection_id LIKE '%${searchTerm}%'
         OR channel_id LIKE '%${searchTerm}%'
         OR timestamp LIKE '%${searchTerm}%'
         OR rtc_type LIKE '%${searchTerm}%'
      USING SAMPLE 1 PERCENT (bernoulli);
    `;
    return await this.executeQuery(query);
  }

  public async downloadSampleParquet(): Promise<Uint8Array | null> {
    if (!this.db) {
      throw new Error("DB is not initialized");
    }
    try {
      const conn = await this.db.connect();
      await conn.query(`
        COPY (SELECT * FROM rtc_stats
        USING SAMPLE 1 PERCENT (bernoulli)) 
        TO '${this.config.sampleParquetFileName}' (FORMAT 'parquet', COMPRESSION 'zstd');
      `);
      const parquetBuffer = await this.db.copyFileToBuffer(
        this.config.sampleParquetFileName,
      );
      await conn.close();

      return parquetBuffer;
    } catch (error) {
      console.error("Parquet ダウンロード時エラー:", error);
      return null;
    }
  }
}
