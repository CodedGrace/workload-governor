/**
 * eventIndexer.ts
 *
 * Polls the Soroban RPC node every 5 seconds for WorkloadGovernor contract events,
 * parses them into typed DB records, and persists them with deduplication.
 *
 * Supported event types (matching src/events.rs emit helpers):
 *   applied, withdrew, assigned, completed, revoked, maint_reg
 *
 * ## 3-element topic schema (#829 SC-004)
 *
 * All contract events now emit a standardised 3-element topic tuple:
 *   topics[0]  Symbol("WG")          — contract namespace discriminant
 *   topics[1]  Symbol(<event_name>)  — operation identifier
 *   topics[2]  Address               — primary entity (contributor or admin)
 *
 * The indexer filters on topics[0] == "WG" to efficiently locate WorkloadGovernor
 * events without wildcard scans, and reads the event type from topics[1].
 *
 * Deduplication key: (ledger_sequence, transaction_hash, event_index) —
 *   INSERT … ON CONFLICT DO NOTHING prevents duplicate rows even after a
 *   full-history replay caused by cursor loss (fixes issue #575).
 *
 * Resume: on startup, reads the highest ledger already stored and continues
 *   from there, relying on the unique constraint to skip already-seen events.
 */

import { SorobanRpc } from '@stellar/stellar-sdk';
import { pool, saveCheckpoint } from './db';
import { parseRpcEvent } from './eventDecoder';
import type { ContractEventRecord, RpcEvent } from './eventDecoder';
import { logger } from './logger';
import { indexerLastProcessedLedger, indexerLedgerLag } from './metrics';
import { publishLiveEvent } from './services/event-bus';

export type { ContractEventType, ContractEventRecord } from './eventDecoder';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const CONTRACT_ID =
  process.env['CONTRACT_ID'] ??
  'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4';

const RPC_URL =
  process.env['SOROBAN_RPC_URL'] ?? 'https://soroban-testnet.stellar.org';

const POLL_INTERVAL_MS = 5_000;
const ERROR_BACKOFF_MS = 10_000;

// ---------------------------------------------------------------------------
// EventIndexer class
// ---------------------------------------------------------------------------

export class EventIndexer {
  private server: SorobanRpc.Server;
  /** Paging cursor for the next RPC call. Undefined means start from resume ledger. */
  private cursor: string | undefined;
  private isRunning = false;

  constructor() {
    this.server = new SorobanRpc.Server(RPC_URL, { allowHttp: true });
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    // Resume from the last successfully indexed ledger
    await this.initCursor();

    logger.info({ message: 'Event indexer started', contract: CONTRACT_ID, rpc: RPC_URL });

    this.pollForEvents().catch((err) => {
      logger.error({
        message: 'Event indexer fatal error',
        error: err instanceof Error ? err.message : String(err),
        stack: err instanceof Error ? err.stack : undefined,
      });
      this.isRunning = false;
    });
  }

  stop(): void {
    this.isRunning = false;
    logger.info({ message: 'Event indexer stopped' });
  }

  // ── Cursor / resume ──────────────────────────────────────────────────────

  /**
   * On restart, re-process from the last finalized ledger to handle reorgs.
   * Uses the highest ledger_seq stored in contract_events as the resume point.
   *
   * Because INSERT uses ON CONFLICT DO NOTHING, replaying events from the
   * resume ledger is safe — already-indexed events are silently skipped.
   * This prevents duplicates even if the cursor is lost (fixes issue #575).
   */
  private async initCursor(): Promise<void> {
    try {
      const { rows } = await pool.query<{ max_ledger: string | null }>(
        'SELECT MAX(ledger_seq) AS max_ledger FROM contract_events',
      );
      const maxLedger = rows[0]?.max_ledger != null ? parseInt(rows[0].max_ledger, 10) : null;

      if (maxLedger !== null && maxLedger > 0) {
        // Use the last finalized ledger as the start cursor so we re-fetch
        // that ledger's events and handle any potential reorg.
        // The cursor format expected by getEvents is "<ledger>-<tx>-<event>"
        // Passing just the ledger number as a numeric string is also accepted.
        this.cursor = String(maxLedger);
        logger.info({ message: 'Resuming indexer from ledger', ledger: maxLedger });
      } else {
        this.cursor = undefined;
        logger.info({ message: 'Starting indexer from genesis (no stored events)' });
      }
    } catch (err) {
      // Table might not exist yet; start from genesis
      logger.warn({
        message: 'Could not read resume ledger, starting from genesis',
        error: err instanceof Error ? err.message : String(err),
      });
      this.cursor = undefined;
    }
  }

  // ── Poll loop ────────────────────────────────────────────────────────────

  private async pollForEvents(): Promise<void> {
    while (this.isRunning) {
      try {
        const response = await this.server.getEvents({
          filters: [
            {
              type: 'contract',
              contractIds: [CONTRACT_ID],
            },
          ],
          cursor: this.cursor,
          limit: 200,
        });

        const events = response.events as unknown as RpcEvent[];

        if (events.length > 0) {
          const last = events[events.length - 1];
          const records = events
            .map(parseRpcEvent)
            .filter((record): record is ContractEventRecord => record !== null);
          const inserted = await this.storeBatch(
            records,
            last,
            response.latestLedger,
          );

          this.cursor = last.pagingToken ?? last.id;

          logger.info({
            message: 'Indexed event batch',
            ledger: Number(last.ledger),
            total: events.length,
            persisted: inserted.length,
            skipped: events.length - inserted.length,
          });
        }

        await sleep(POLL_INTERVAL_MS);
      } catch (err) {
        logger.error({
          message: 'Event polling error',
          error: err instanceof Error ? err.message : String(err),
        });
        await sleep(ERROR_BACKOFF_MS);
      }
    }
  }

  // ── Persistence ──────────────────────────────────────────────────────────

  private async storeBatch(
    records: ContractEventRecord[],
    lastEvent: RpcEvent,
    latestLedger: number,
  ): Promise<ContractEventRecord[]> {
    const client = await pool.connect();
    let inserted: ContractEventRecord[] = [];
    const lastLedger = Number(lastEvent.ledger);

    try {
      await client.query('BEGIN');
      if (records.length > 0) {
        const parameters: unknown[] = [];
        const values = records.map((record, index) => {
          const offset = index * 8;
          parameters.push(
            record.event_type,
            record.contributor,
            record.org_id,
            record.issue_id,
            record.tx_hash,
            record.event_index,
            record.ledger,
            record.timestamp,
          );
          return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, ` +
            `$${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8})`;
        });
        const result = await client.query<{ tx_hash: string; event_index: number }>(
          `INSERT INTO contract_events
             (event_type, contributor, org_id, issue_id, tx_hash, event_index, ledger_seq, timestamp)
           VALUES ${values.join(', ')}
           ON CONFLICT (ledger_seq, tx_hash, event_index) DO NOTHING
           RETURNING tx_hash, event_index`,
          parameters,
        );
        const insertedKeys = new Set(
          result.rows.map(({ tx_hash, event_index }) => JSON.stringify([tx_hash, event_index])),
        );
        inserted = records.filter((record) =>
          insertedKeys.has(JSON.stringify([record.tx_hash, record.event_index])),
        );
      }
      await saveCheckpoint(CONTRACT_ID, lastLedger, lastEvent.txHash ?? null, client);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    indexerLastProcessedLedger.set(lastLedger);
    indexerLedgerLag.set(Math.max(0, latestLedger - lastLedger));

    for (const record of inserted) {
      const liveType =
        record.event_type === 'applied'
          ? 'application_created'
          : record.event_type === 'assigned'
            ? 'assignment_created'
            : 'cap_updated';
      publishLiveEvent({
        type: liveType,
        data: {
          eventType: record.event_type,
          orgId: record.org_id,
          issueId: record.issue_id,
        },
      });
    }

    return inserted;
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Module-level singleton helpers
// ---------------------------------------------------------------------------

let _indexer: EventIndexer | null = null;

export function getEventIndexer(): EventIndexer {
  if (!_indexer) _indexer = new EventIndexer();
  return _indexer;
}

export async function startEventIndexer(): Promise<void> {
  await getEventIndexer().start();
}

export function stopEventIndexer(): void {
  _indexer?.stop();
}
