import { Request, Response } from 'express';
import { LogItemSchema, LogItem } from '../schemas/log.schema.js';
import { pool } from '../db/index.js';

const TARGET_FLUSH_SIZE = 4000;
const MAX_CONCURRENT_FLUSHES = 3;

interface PendingBatch {
    entries: LogItem[];
    resolve: () => void;
    reject: (error: unknown) => void;
}

function truncateToMinuteISO(isoTimestamp: string): string {
    const d = new Date(isoTimestamp);
    d.setUTCSeconds(0, 0);
    return d.toISOString();
}

function buildLogsInsertParams(entries: LogItem[]) {
    const timestamps: string[] = [];
    const levels: string[] = [];
    const services: string[] = [];
    const messages: string[] = [];
    const attributes: string[] = [];

    for (const log of entries) {
        timestamps.push(log.timestamp);
        levels.push(log.level);
        services.push(log.service);
        messages.push(log.message);
        attributes.push(JSON.stringify(log.attributes || {}));
    }

    return { timestamps, levels, services, messages, attributes };
}

function buildRollupUpsertParams(entries: LogItem[]) {
    const map = new Map<string, { bucket: string; service: string; level: string; count: number }>();
    for (const log of entries) {
        const bucket = truncateToMinuteISO(log.timestamp);
        const key = `${bucket}|${log.service}|${log.level}`;
        const existing = map.get(key);
        if (existing) {
            existing.count++;
        } else {
            map.set(key, { bucket, service: log.service, level: log.level, count: 1 });
        }
    }

    const rows = Array.from(map.values()).sort(
        (a, b) =>
            a.bucket.localeCompare(b.bucket) ||
            a.service.localeCompare(b.service) ||
            a.level.localeCompare(b.level)
    );

    return {
        buckets: rows.map((r) => r.bucket),
        services: rows.map((r) => r.service),
        levels: rows.map((r) => r.level),
        counts: rows.map((r) => r.count),
    };
}

async function persistEntries(entries: LogItem[]): Promise<void> {
    if (entries.length === 0) return;

    const client = await pool.connect();
    try {
        for (let attempt = 1; ; attempt++) {
            try {
                await client.query('BEGIN');

                const { timestamps, levels, services, messages, attributes } = buildLogsInsertParams(entries);
                await client.query(
                    `INSERT INTO logs (timestamp, level, service, message, attributes)
                     SELECT t::timestamptz, l, s, m, a::jsonb
                     FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
                     AS x(t, l, s, m, a);`,
                    [timestamps, levels, services, messages, attributes]
                );

                const { buckets, services: rServices, levels: rLevels, counts } = buildRollupUpsertParams(entries);
                await client.query(
                    `INSERT INTO logs_rollup_1m (bucket_start, service, level, count)
                     SELECT b::timestamptz, s, l, c::bigint
                     FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[]) AS x(b, s, l, c)
                     ON CONFLICT (bucket_start, service, level)
                     DO UPDATE SET count = logs_rollup_1m.count + EXCLUDED.count;`,
                    [buckets, rServices, rLevels, counts]
                );

                await client.query('COMMIT');
                return;
            } catch (error) {
                await client.query('ROLLBACK');
                const isDeadlock =
                    attempt < 3 &&
                    typeof error === 'object' &&
                    error !== null &&
                    (error as { code?: string }).code === '40P01';
                if (!isDeadlock) throw error;
            }
        }
    } finally {
        client.release();
    }
}

class IngestBatcher {
    private queue: PendingBatch[] = [];
    private activeFlushes = 0;
    private waiters: Array<() => void> = [];

    save(entries: LogItem[]): Promise<void> {
        if (entries.length === 0) return Promise.resolve();

        return new Promise<void>((resolve, reject) => {
            this.queue.push({ entries, resolve, reject });
            this.wakeAll();
            this.spawnFlush();
        });
    }

    private wakeAll() {
        const w = this.waiters;
        this.waiters = [];
        w.forEach((fn) => fn());
    }

    private spawnFlush() {
        if (this.activeFlushes >= MAX_CONCURRENT_FLUSHES || this.queue.length === 0) return;

        this.activeFlushes++;
        this.runFlushLoop().finally(() => {
            this.activeFlushes--;
            this.spawnFlush();
        });
    }

    private async runFlushLoop() {
        while (this.queue.length > 0) {
            const batches = this.takeBatches();
            if (batches.length === 0) break;
            await this.persist(batches);
        }
    }

    private takeBatches(): PendingBatch[] {
        const batches: PendingBatch[] = [];
        let count = 0;
        while (this.queue.length > 0) {
            const next = this.queue[0];
            if (count > 0 && count + next.entries.length > TARGET_FLUSH_SIZE) break;
            this.queue.shift();
            batches.push(next);
            count += next.entries.length;
        }
        if (batches.length === 0 && this.queue.length > 0) {
            batches.push(this.queue.shift()!);
        }
        return batches;
    }

    private async persist(batches: PendingBatch[]) {
        const entries = batches.flatMap((b) => b.entries);
        try {
            await persistEntries(entries);
            batches.forEach((b) => b.resolve());
        } catch (error) {
            console.error('Error persisting log batch:', error);
            batches.forEach((b) => b.reject(error));
        }
    }
}