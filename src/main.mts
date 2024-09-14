import * as duckdb from '@duckdb/duckdb-wasm'
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url'
import duckdb_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?worker'

document.addEventListener('DOMContentLoaded', async () => {
  const PARQUET_FILE_URL = import.meta.env.VITE_PARQUET_FILE_URL
  const scanParquetButton = document.getElementById('scan-parquet') as HTMLButtonElement | null
  const samplesButton = document.getElementById('samples') as HTMLButtonElement | null
  const aggregationButton = document.getElementById('aggregation') as HTMLButtonElement | null
  const clearButton = document.getElementById('clear') as HTMLButtonElement | null

  // すべてのボタンを初期状態で無効化
  for (const button of [scanParquetButton, samplesButton, aggregationButton, clearButton]) {
    if (button) button.disabled = true
  }

  const worker = new duckdb_worker()
  const logger = new duckdb.ConsoleLogger()
  const db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(duckdb_wasm)

  const conn = await db.connect()
  await conn.query(`
    INSTALL parquet;
    LOAD parquet;
    INSTALL json;
    LOAD json;
  `)
  await conn.close()

  // DuckDBの初期化が完了したらボタンを有効化
  if (scanParquetButton) {
    scanParquetButton.disabled = false
  }

  document.getElementById('scan-parquet')?.addEventListener('click', async () => {
    const buffer = await getParquetBuffer(PARQUET_FILE_URL)

    await db.registerFileBuffer('rtc_stats.parquet', new Uint8Array(buffer))

    const conn = await db.connect()
    await conn.query(`
      INSTALL parquet;
      LOAD parquet;
      CREATE TABLE rtc_stats AS SELECT *
      FROM read_parquet('rtc_stats.parquet');
    `)

    const scannedElement = document.getElementById('scanned')
    if (scannedElement) {
      scannedElement.textContent = 'Scanned: true'
    }

    const result = await conn.query(`
      SELECT count(*) AS count FROM rtc_stats;
    `)

    const resultElement = document.getElementById('counted')
    if (resultElement) {
      resultElement.textContent = `Count: ${result.toArray()[0].count}`
    }

    // scan-parquetボタンを無効化し、他のボタンを有効化
    if (scanParquetButton) {
      scanParquetButton.disabled = true
    }
    if (samplesButton) {
      samplesButton.disabled = false
    }
    if (aggregationButton) {
      aggregationButton.disabled = false
    }
    if (clearButton) {
      clearButton.disabled = false
    }

    await conn.close()
  })

  document.getElementById('samples')?.addEventListener('click', async () => {
    const conn = await db.connect()
    // 10% のサンプルを取得
    const result = await conn.query(`
      SELECT timestamp, connection_id, rtc_type
      FROM rtc_stats
      USING SAMPLE 1 PERCENT (bernoulli);
    `)

    const resultElement = document.getElementById('result')
    if (resultElement) {
      const table = document.createElement('table')
      const headers = ['timestamp', 'connection_id', 'rtc_type']

      const headerRow = document.createElement('tr')
      headerRow.innerHTML = headers.map((header) => `<th>${header}</th>`).join('')
      table.appendChild(headerRow)

      const rows = result.toArray().map((row) => {
        const parsedRow = JSON.parse(row)
        const tr = document.createElement('tr')
        tr.innerHTML = headers.map((header) => `<td>${parsedRow[header]}</td>`).join('')
        return tr
      })

      table.append(...rows)

      resultElement.innerHTML = ''
      resultElement.appendChild(table)
    }

    await conn.close()
  })

  document.getElementById('aggregation')?.addEventListener('click', async () => {
    const conn = await db.connect()
    // SQL は適当ですので、参考にしないで下さい
    const result = await conn.query(`
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
        ORDER BY time_bucket ASC;
    `)

    const resultElement = document.getElementById('result')
    if (resultElement) {
      const table = document.createElement('table')
      const headers = [
        'time_bucket',
        'channel_id',
        'session_id',
        'connection_id',
        'bytes_sent_diff',
        'bytes_received_diff',
        'packets_sent_diff',
        'packets_received_diff',
      ]

      const headerRow = document.createElement('tr')
      headerRow.innerHTML = headers.map((header) => `<th>${header}</th>`).join('')
      table.appendChild(headerRow)

      const rows = result.toArray().map((row) => {
        const parsedRow = JSON.parse(row)
        const tr = document.createElement('tr')
        tr.innerHTML = headers.map((header) => `<td>${parsedRow[header]}</td>`).join('')
        return tr
      })

      table.append(...rows)

      resultElement.innerHTML = ''
      resultElement.appendChild(table)
    }
  })

  document.getElementById('clear')?.addEventListener('click', async () => {
    const resultElement = document.getElementById('result')
    if (resultElement) {
      resultElement.innerHTML = ''
    }

    // DuckDB からテーブルを削除
    const conn = await db.connect()
    await conn.query('DROP TABLE IF EXISTS rtc_stats;')
    await conn.close()

    // DuckDB からファイルを削除
    await db.dropFile('rtc_stats.parquet')

    // OPFS からファイルを削除
    if ('createWritable' in FileSystemFileHandle.prototype) {
      try {
        await deleteBufferFromOPFS()
        console.log('Parquet file deleted from OPFS')
      } catch (error) {
        console.error('Error deleting Parquet file from OPFS:', error)
      }
    }

    const scannedElement = document.getElementById('scanned')
    if (scannedElement) {
      scannedElement.textContent = 'Scanned: false'
    }

    const countedElement = document.getElementById('counted')
    if (countedElement) {
      countedElement.textContent = 'Counted: 0'
    }

    // ボタンの状態を更新
    if (scanParquetButton) {
      scanParquetButton.disabled = false
    }
    if (samplesButton) {
      samplesButton.disabled = true
    }
    if (aggregationButton) {
      aggregationButton.disabled = true
    }
  })
})

// OPFS関連の関数

const FILE_NAME = 'rtc_stats.parquet'

const getBufferFromOPFS = async (): Promise<ArrayBuffer | null> => {
  try {
    const root = await navigator.storage.getDirectory()
    const fileHandle = await root.getFileHandle(FILE_NAME)
    const file = await fileHandle.getFile()
    return await file.arrayBuffer()
  } catch (error) {
    console.error('Error reading file from OPFS:', error)
    return null
  }
}

const saveBufferToOPFS = async (buffer: ArrayBuffer): Promise<void> => {
  if ('createWritable' in FileSystemFileHandle.prototype) {
    try {
      const root = await navigator.storage.getDirectory()
      const fileHandle = await root.getFileHandle(FILE_NAME, { create: true })
      const writable = await fileHandle.createWritable()
      await writable.write(buffer)
      await writable.close()
    } catch (error) {
      console.error('Error saving file to OPFS:', error)
      throw error
    }
  } else {
    console.warn('createWritable is not supported. Data will not be saved to OPFS.')
  }
}

const deleteBufferFromOPFS = async (): Promise<void> => {
  try {
    const root = await navigator.storage.getDirectory()
    await root.removeEntry(FILE_NAME)
  } catch (error) {
    console.error('Error deleting file from OPFS:', error)
    throw error
  }
}

const getParquetBuffer = async (PARQUET_FILE_URL: string): Promise<ArrayBuffer> => {
  let buffer = null
  if ('createWritable' in FileSystemFileHandle.prototype) {
    buffer = await getBufferFromOPFS()
  }

  if (!buffer) {
    const response = await fetch(PARQUET_FILE_URL)
    buffer = await response.arrayBuffer()
    if ('createWritable' in FileSystemFileHandle.prototype) {
      await saveBufferToOPFS(buffer)
    }
  }

  return buffer
}
