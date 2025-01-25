import { useEffect, useRef, useState } from "preact/hooks";
import "@/styles/home.scss";
import {
  DuckDbExchangeRateHandler,
  IDuckDBHandler,
  DuckDBConfig,
} from "@/utils/duckdb";
import Editor from "@/components/editor";

export default function Exchangerate() {
  const duckdbConfig: DuckDBConfig = {
    dbPath: "opfs://duckdbwasm-exchangerate-example.db",
    parquetFileName: "exrate.parquet",
    sampleParquetFileName: "exrate_sample.parquet",
    parquetSourceUrl: "",
  };

  const [exchangerateHandler] = useState<IDuckDBHandler>(
    () => new DuckDbExchangeRateHandler(duckdbConfig),
  );

  const [duckdbVersion, setDuckdbVersion] = useState<string>("");
  const [duckdbWasmVersion, setDuckdbWasmVersion] = useState<string>("");
  const [isOPFS, setIsOPFS] = useState<boolean>(false);
  const [counted, setCounted] = useState<number>(0);
  const [isVimMode, setIsVimMode] = useState<boolean>(true);
  const [resultHeaders, setResultHeaders] = useState<string[]>([]);
  const [resultRows, setResultRows] = useState<any[]>([]);
  const [downloadUrl, setDownloadUrl] = useState<string>("");
  const [editorValue, setEditorValue] = useState<string>("");
  const [autoDownload, setAutoDownload] = useState<boolean>(false);
  const downloadRef = useRef<HTMLAnchorElement>(null);

  useEffect(() => {
    if (autoDownload && downloadRef.current) {
      downloadRef.current.click();
      setAutoDownload(false);
    }
  }, [autoDownload]);

  useEffect(() => {
    (async () => {
      try {
        await exchangerateHandler.init();

        const version = await exchangerateHandler.getVersion();
        setDuckdbVersion(version);
        setDuckdbWasmVersion(exchangerateHandler.getDuckDBWasmVersion());

        const defaultQuery = exchangerateHandler.getDefaultQuery();
        setEditorValue(defaultQuery);

        await exchangerateHandler.register();

        const rc = await exchangerateHandler.getRecordCount();
        setCounted(rc);

        setIsOPFS(true);
      } catch (error) {
        console.error("DuckDB 初期化エラー:", error);
      }
    })();
  }, [exchangerateHandler]);

  const handleExecuteQuery = async (query: string) => {
    try {
      const { headers, rows } = await exchangerateHandler.executeQuery(query);
      setResultHeaders(headers);
      setResultRows(rows);
    } catch (error) {
      console.error("Query実行エラー:", error);
    }
  };

  const handleFetchParquet = async () => {
    try {
      await exchangerateHandler.register();
      const rc = await exchangerateHandler.getRecordCount();
      setCounted(rc);
      setIsOPFS(true);
    } catch (error) {
      console.error("fetchParquet エラー:", error);
    }
  };

  const handleSamples = async () => {
    const query = `
      SELECT timestamp, connection_id, rtc_type
      FROM rtc_stats
      USING SAMPLE 1 PERCENT (bernoulli);
    `;
    await handleExecuteQuery(query);
  };

  const handleSamplesDownloadParquet = async () => {
    try {
      const parquetBuffer = await exchangerateHandler.downloadSampleParquet();
      if (!parquetBuffer) return;

      const blob = new Blob([parquetBuffer], {
        type: "application/octet-stream",
      });
      const url = URL.createObjectURL(blob);

      setDownloadUrl(url);
      setAutoDownload(true);
    } catch (error) {
      console.error("Parquet ダウンロード時エラー:", error);
    }
  };

  const handleAggregation = async () => {
    await handleExecuteQuery(exchangerateHandler.getDefaultQuery());
  };

  const handlePurge = async () => {
    setResultHeaders([]);
    setResultRows([]);
    await exchangerateHandler.purge();

    setIsOPFS(false);
    setCounted(0);
    setDuckdbVersion("");
    setDuckdbWasmVersion("");
  };

  const [searchTerm, setSearchTerm] = useState("");
  const handleSearchInput = async (value: string) => {
    setSearchTerm(value);
    if (!value.trim()) {
      setResultHeaders([]);
      setResultRows([]);
      return;
    }
    try {
      const { headers, rows } = await exchangerateHandler.search(value);
      setResultHeaders(headers);
      setResultRows(rows);
    } catch (error) {
      console.error("Search 実行エラー:", error);
      setResultHeaders([]);
      setResultRows([]);
    }
  };

  const handleToggleVim = () => {
    setIsVimMode((prev) => !prev);
  };

  const handleRunEditorQuery = async () => {
    if (!editorValue) return;
    await handleExecuteQuery(editorValue);
  };

  return (
    <div class="exchangerate-container">
      <div class="button-group">
        <button onClick={handleFetchParquet} disabled={!duckdbVersion}>
          fetch-parquet
        </button>
        <button onClick={handleSamples} disabled={!duckdbVersion}>
          samples
        </button>
        <button
          onClick={handleSamplesDownloadParquet}
          disabled={!duckdbVersion}
        >
          download parquet
        </button>
        <button onClick={handleAggregation} disabled={!duckdbVersion}>
          aggregation
        </button>
        <button onClick={handlePurge} disabled={!duckdbVersion}>
          purge
        </button>
        <input
          type="text"
          placeholder="search..."
          value={searchTerm}
          onInput={(e) =>
            handleSearchInput((e.target as HTMLInputElement).value)
          }
          disabled={!duckdbVersion}
        />
      </div>

      <div class="cm-option-buttons">
        <button onClick={handleToggleVim}>
          {isVimMode ? "Normal Mode" : "Vim Mode"}
        </button>
        <button onClick={handleRunEditorQuery} disabled={!duckdbVersion}>
          Run Query
        </button>
      </div>

      <div class="duckdb-info">
        <div>DuckDB: {duckdbVersion || "N/A"}</div>
        <div>DuckDB-Wasm: {duckdbWasmVersion || "N/A"}</div>
        <div>OPFS: {isOPFS ? "true" : "false"}</div>
        <div>Counted: {counted}</div>
      </div>

      <Editor
        initialValue={editorValue}
        isVimMode={isVimMode}
        onChange={(val) => setEditorValue(val)}
      />

      <div class="result-container">
        {resultHeaders.length > 0 && resultRows.length > 0 ? (
          <table class="result-table">
            <thead>
              <tr>
                {resultHeaders.map((header) => (
                  <th key={header}>{header}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {resultRows.map((rowObj, rowIdx) => (
                <tr key={rowIdx}>
                  {resultHeaders.map((header) => (
                    <td key={header}>{rowObj[header]}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div class="no-result">No results.</div>
        )}
      </div>

      {downloadUrl && (
        <a
          href={downloadUrl}
          download="samples.parquet"
          style="display: none;"
          ref={downloadRef}
        >
          Download
        </a>
      )}
    </div>
  );
}
