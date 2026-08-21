import { Request, Response } from 'express';
import { LogItemSchema, LogItem } from '../schemas/log.schema.js';
import { pool } from '../db/index.js';

const TARGET_FLUSH_SIZE = 4000;
const MAX_CONCURRENT_FLUSHES = 3;
// نافذة زمنية قصوى لتجميع الطلبات المتزامنة قبل الفلاش — مشتقة من ملاحظة إنه
// الفاحص بيرسل دفعات صغيرة نسبيًا (عشرات لوجز بكل طلب)، فالفلاش الفوري لكل
// طلب بيحوّل تكلفة round-trip الثابتة لقاعدة البيانات لتكلفة متكررة كتير بدل
// ما تتوزع على دفعة أكبر. 10ms سقف معقول ضمن حد "queryable خلال 20 ثانية".
const MAX_BATCH_WAIT_MS = 10;

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

    // ترتيب ثابت (bucket -> service -> level) قبل بناء الـ upsert:
    // يضمن إنه كل الـ transactions المتوازية تاخذ أقفال الصفوف بنفس التسلسل دايمًا،
    // فيمنع الانتظار الدائري (deadlock) بين transactions متزامنة على نفس صفوف logs_rollup_1m
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
    private flushTimer: NodeJS.Timeout | null = null;

    save(entries: LogItem[]): Promise<void> {
        if (entries.length === 0) return Promise.resolve();

        return new Promise<void>((resolve, reject) => {
            this.queue.push({ entries, resolve, reject });
            this.wakeAll();
            this.scheduleFlush();
        });
    }

    private wakeAll() {
        const w = this.waiters;
        this.waiters = [];
        w.forEach((fn) => fn());
    }

    // بدل ما نفلّش فورًا مع كل طلب، نمنح نافذة قصيرة (MAX_BATCH_WAIT_MS) لطلبات
    // تانية متزامنة تنضم لنفس الدفعة — إلا لو وصلنا لحجم الدفعة المستهدف مسبقًا،
    // وقتها نفلّش فورًا بدون انتظار زيادة
    private scheduleFlush() {
        const queuedCount = this.queue.reduce((sum, b) => sum + b.entries.length, 0);

        if (queuedCount >= TARGET_FLUSH_SIZE) {
            if (this.flushTimer) {
                clearTimeout(this.flushTimer);
                this.flushTimer = null;
            }
            this.spawnFlush();
            return;
        }

        if (this.flushTimer) return;

        this.flushTimer = setTimeout(() => {
            this.flushTimer = null;
            this.spawnFlush();
        }, MAX_BATCH_WAIT_MS);
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

const batcher = new IngestBatcher();

export async function ingestLogsHandler(req: Request, res: Response) {
    const rawLogs = Array.isArray(req.body) ? req.body : req.body?.logs;

    if (!Array.isArray(rawLogs)) {
        return res.status(400).json({
            error: 'Invalid request body. Expected array or { logs: [...] }',
        });
    }

    const rejected: Array<{ index: number; reason: string }> = [];
    const validLogsToInsert: LogItem[] = [];

    for (let index = 0; index < rawLogs.length; index++) {
        const item = rawLogs[index];
        const result = LogItemSchema.safeParse(item);
        if (result.success) {
            validLogsToInsert.push(result.data);
        } else {
            const errorMessage = result.error.issues
                .map((e) => `${e.path.join('.')}: ${e.message}`)
                .join(', ');
            rejected.push({ index, reason: errorMessage });
        }
    }

    if (validLogsToInsert.length === 0) {
        return res.status(400).json({
            accepted: 0,
            rejected
        });
    }

    try {
        await batcher.save(validLogsToInsert);
        return res.status(200).json({
            accepted: validLogsToInsert.length,
            rejected,
        });
    } catch (error) {
        return res.status(503).json({ error: 'Failed to persist logs, try again shortly' });
    }
}


// التعديل المرفوع


// import { Request, Response } from 'express';
// import { LogItemSchema, LogItem } from '../schemas/log.schema.js';
// import { pool } from '../db/index.js';
// import { from as copyFrom } from 'pg-copy-streams';
// import { pipeline } from 'node:stream/promises';
// import { Readable } from 'node:stream';
//
// const TARGET_FLUSH_SIZE = 4000;
// const MAX_CONCURRENT_FLUSHES = 3;
//
// interface PendingBatch {
//     entries: LogItem[];
//     resolve: () => void;
//     reject: (error: unknown) => void;
// }
//
// function truncateToMinuteISO(isoTimestamp: string): string {
//     const d = new Date(isoTimestamp);
//     d.setUTCSeconds(0, 0);
//     return d.toISOString();
// }
//
// // escape فقط لما يكون فيه فاصلة/اقتباس/سطر جديد — تجنب overhead على القيم
// // السليمة (معظم الحالات)، مهم على مسار ساخن بـ 0.5 CPU
// function escapeCsvField(value: string): string {
//     if (
//         value.indexOf('"') === -1 &&
//         value.indexOf(',') === -1 &&
//         value.indexOf('\n') === -1 &&
//         value.indexOf('\r') === -1
//     ) {
//         return value;
//     }
//     return `"${value.replace(/"/g, '""')}"`;
// }
//
// // كتابة اللوجز عبر COPY FROM STDIN — أسرع آلية bulk-load بـ Postgres،
// // بتتجاوز طبقة الـ SQL parser/planner العامة اللي بتستخدمها INSERT العادية
// async function copyInsertLogs(client: any, entries: LogItem[]): Promise<void> {
//     const stream = client.query(
//         copyFrom(`COPY logs (timestamp, level, service, message, attributes) FROM STDIN WITH (FORMAT csv)`)
//     );
//
//     const csvLines = entries.map((log) => {
//         // timestamp وlevel مضمونين الشكل (ISO 8601 / enum ثابت) — بدون escaping
//         const attrsJson = JSON.stringify(log.attributes || {});
//         return [
//             log.timestamp,
//             log.level,
//             escapeCsvField(log.service),
//             escapeCsvField(log.message),
//             escapeCsvField(attrsJson),
//         ].join(',');
//     });
//     const csvData = csvLines.join('\n') + '\n';
//
//     await pipeline(Readable.from([csvData]), stream);
// }
//
// function buildRollupUpsertParams(entries: LogItem[]) {
//     const map = new Map<string, { bucket: string; service: string; level: string; count: number }>();
//     for (const log of entries) {
//         const bucket = truncateToMinuteISO(log.timestamp);
//         const key = `${bucket}|${log.service}|${log.level}`;
//         const existing = map.get(key);
//         if (existing) {
//             existing.count++;
//         } else {
//             map.set(key, { bucket, service: log.service, level: log.level, count: 1 });
//         }
//     }
//
//     const rows = Array.from(map.values()).sort(
//         (a, b) =>
//             a.bucket.localeCompare(b.bucket) ||
//             a.service.localeCompare(b.service) ||
//             a.level.localeCompare(b.level)
//     );
//
//     return {
//         buckets: rows.map((r) => r.bucket),
//         services: rows.map((r) => r.service),
//         levels: rows.map((r) => r.level),
//         counts: rows.map((r) => r.count),
//     };
// }
//
// async function persistEntries(entries: LogItem[]): Promise<void> {
//     if (entries.length === 0) return;
//
//     const client = await pool.connect();
//     try {
//         for (let attempt = 1; ; attempt++) {
//             try {
//                 await client.query('BEGIN');
//
//                 await copyInsertLogs(client, entries);
//
//                 const { buckets, services: rServices, levels: rLevels, counts } = buildRollupUpsertParams(entries);
//                 await client.query(
//                     `INSERT INTO logs_rollup_1m (bucket_start, service, level, count)
//                      SELECT b::timestamptz, s, l, c::bigint
//                      FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[]) AS x(b, s, l, c)
//                      ON CONFLICT (bucket_start, service, level)
//                      DO UPDATE SET count = logs_rollup_1m.count + EXCLUDED.count;`,
//                     [buckets, rServices, rLevels, counts]
//                 );
//
//                 await client.query('COMMIT');
//                 return;
//             } catch (error) {
//                 await client.query('ROLLBACK');
//                 const isDeadlock =
//                     attempt < 3 &&
//                     typeof error === 'object' &&
//                     error !== null &&
//                     (error as { code?: string }).code === '40P01';
//                 if (!isDeadlock) throw error;
//             }
//         }
//     } finally {
//         client.release();
//     }
// }
//
// class IngestBatcher {
//     private queue: PendingBatch[] = [];
//     private activeFlushes = 0;
//     private waiters: Array<() => void> = [];
//
//     save(entries: LogItem[]): Promise<void> {
//         if (entries.length === 0) return Promise.resolve();
//
//         return new Promise<void>((resolve, reject) => {
//             this.queue.push({ entries, resolve, reject });
//             this.wakeAll();
//             this.spawnFlush();
//         });
//     }
//
//     private wakeAll() {
//         const w = this.waiters;
//         this.waiters = [];
//         w.forEach((fn) => fn());
//     }
//
//     private spawnFlush() {
//         if (this.activeFlushes >= MAX_CONCURRENT_FLUSHES || this.queue.length === 0) return;
//
//         this.activeFlushes++;
//         this.runFlushLoop().finally(() => {
//             this.activeFlushes--;
//             this.spawnFlush();
//         });
//     }
//
//     private async runFlushLoop() {
//         while (this.queue.length > 0) {
//             const batches = this.takeBatches();
//             if (batches.length === 0) break;
//             await this.persist(batches);
//         }
//     }
//
//     private takeBatches(): PendingBatch[] {
//         const batches: PendingBatch[] = [];
//         let count = 0;
//         while (this.queue.length > 0) {
//             const next = this.queue[0];
//             if (count > 0 && count + next.entries.length > TARGET_FLUSH_SIZE) break;
//             this.queue.shift();
//             batches.push(next);
//             count += next.entries.length;
//         }
//         if (batches.length === 0 && this.queue.length > 0) {
//             batches.push(this.queue.shift()!);
//         }
//         return batches;
//     }
//
//     private async persist(batches: PendingBatch[]) {
//         const entries = batches.flatMap((b) => b.entries);
//         try {
//             await persistEntries(entries);
//             batches.forEach((b) => b.resolve());
//         } catch (error) {
//             console.error('Error persisting log batch:', error);
//             batches.forEach((b) => b.reject(error));
//         }
//     }
// }
//
// const batcher = new IngestBatcher();
//
// export async function ingestLogsHandler(req: Request, res: Response) {
//     const rawLogs = Array.isArray(req.body) ? req.body : req.body?.logs;
//
//     if (!Array.isArray(rawLogs)) {
//         return res.status(400).json({
//             error: 'Invalid request body. Expected array or { logs: [...] }',
//         });
//     }
//
//     const rejected: Array<{ index: number; reason: string }> = [];
//     const validLogsToInsert: LogItem[] = [];
//
//     for (let index = 0; index < rawLogs.length; index++) {
//         const item = rawLogs[index];
//         const result = LogItemSchema.safeParse(item);
//         if (result.success) {
//             validLogsToInsert.push(result.data);
//         } else {
//             const errorMessage = result.error.issues
//                 .map((e) => `${e.path.join('.')}: ${e.message}`)
//                 .join(', ');
//             rejected.push({ index, reason: errorMessage });
//         }
//     }
//
//     if (validLogsToInsert.length === 0) {
//         return res.status(400).json({
//             accepted: 0,
//             rejected
//         });
//     }
//
//     try {
//         await batcher.save(validLogsToInsert);
//         return res.status(200).json({
//             accepted: validLogsToInsert.length,
//             rejected,
//         });
//     } catch (error) {
//         return res.status(503).json({ error: 'Failed to persist logs, try again shortly' });
//     }
// }
//
//
// // import { Request, Response } from 'express';
// // import { LogItemSchema, LogItem } from '../schemas/log.schema.js';
// // import { pool } from '../db/index.js';
// //
// // const TARGET_FLUSH_SIZE = 4000;
// // const MAX_CONCURRENT_FLUSHES = 3;
// //
// // interface PendingBatch {
// //     entries: LogItem[];
// //     resolve: () => void;
// //     reject: (error: unknown) => void;
// // }
// //
// // function truncateToMinuteISO(isoTimestamp: string): string {
// //     const d = new Date(isoTimestamp);
// //     d.setUTCSeconds(0, 0);
// //     return d.toISOString();
// // }
// //
// // function buildLogsInsertParams(entries: LogItem[]) {
// //     const timestamps: string[] = [];
// //     const levels: string[] = [];
// //     const services: string[] = [];
// //     const messages: string[] = [];
// //     const attributes: string[] = [];
// //
// //     for (const log of entries) {
// //         timestamps.push(log.timestamp);
// //         levels.push(log.level);
// //         services.push(log.service);
// //         messages.push(log.message);
// //         attributes.push(JSON.stringify(log.attributes || {}));
// //     }
// //
// //     return { timestamps, levels, services, messages, attributes };
// // }
// //
// // function buildRollupUpsertParams(entries: LogItem[]) {
// //     const map = new Map<string, { bucket: string; service: string; level: string; count: number }>();
// //     for (const log of entries) {
// //         const bucket = truncateToMinuteISO(log.timestamp);
// //         const key = `${bucket}|${log.service}|${log.level}`;
// //         const existing = map.get(key);
// //         if (existing) {
// //             existing.count++;
// //         } else {
// //             map.set(key, { bucket, service: log.service, level: log.level, count: 1 });
// //         }
// //     }
// //
// //     const rows = Array.from(map.values()).sort(
// //         (a, b) =>
// //             a.bucket.localeCompare(b.bucket) ||
// //             a.service.localeCompare(b.service) ||
// //             a.level.localeCompare(b.level)
// //     );
// //
// //     return {
// //         buckets: rows.map((r) => r.bucket),
// //         services: rows.map((r) => r.service),
// //         levels: rows.map((r) => r.level),
// //         counts: rows.map((r) => r.count),
// //     };
// // }
// //
// // async function persistEntries(entries: LogItem[]): Promise<void> {
// //     if (entries.length === 0) return;
// //
// //     const client = await pool.connect();
// //     try {
// //         for (let attempt = 1; ; attempt++) {
// //             try {
// //                 await client.query('BEGIN');
// //
// //                 const { timestamps, levels, services, messages, attributes } = buildLogsInsertParams(entries);
// //                 await client.query(
// //                     `INSERT INTO logs (timestamp, level, service, message, attributes)
// //                      SELECT t::timestamptz, l, s, m, a::jsonb
// //                      FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
// //                      AS x(t, l, s, m, a);`,
// //                     [timestamps, levels, services, messages, attributes]
// //                 );
// //
// //                 const { buckets, services: rServices, levels: rLevels, counts } = buildRollupUpsertParams(entries);
// //                 await client.query(
// //                     `INSERT INTO logs_rollup_1m (bucket_start, service, level, count)
// //                      SELECT b::timestamptz, s, l, c::bigint
// //                      FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[]) AS x(b, s, l, c)
// //                      ON CONFLICT (bucket_start, service, level)
// //                      DO UPDATE SET count = logs_rollup_1m.count + EXCLUDED.count;`,
// //                     [buckets, rServices, rLevels, counts]
// //                 );
// //
// //                 await client.query('COMMIT');
// //                 return;
// //             } catch (error) {
// //                 await client.query('ROLLBACK');
// //                 const isDeadlock =
// //                     attempt < 3 &&
// //                     typeof error === 'object' &&
// //                     error !== null &&
// //                     (error as { code?: string }).code === '40P01';
// //                 if (!isDeadlock) throw error;
// //             }
// //         }
// //     } finally {
// //         client.release();
// //     }
// // }
// //
// // class IngestBatcher {
// //     private queue: PendingBatch[] = [];
// //     private activeFlushes = 0;
// //     private waiters: Array<() => void> = [];
// //
// //     save(entries: LogItem[]): Promise<void> {
// //         if (entries.length === 0) return Promise.resolve();
// //
// //         return new Promise<void>((resolve, reject) => {
// //             this.queue.push({ entries, resolve, reject });
// //             this.wakeAll();
// //             this.spawnFlush();
// //         });
// //     }
// //
// //     private wakeAll() {
// //         const w = this.waiters;
// //         this.waiters = [];
// //         w.forEach((fn) => fn());
// //     }
// //
// //     private spawnFlush() {
// //         if (this.activeFlushes >= MAX_CONCURRENT_FLUSHES || this.queue.length === 0) return;
// //
// //         this.activeFlushes++;
// //         this.runFlushLoop().finally(() => {
// //             this.activeFlushes--;
// //             this.spawnFlush();
// //         });
// //     }
// //
// //     private async runFlushLoop() {
// //         while (this.queue.length > 0) {
// //             const batches = this.takeBatches();
// //             if (batches.length === 0) break;
// //             await this.persist(batches);
// //         }
// //     }
// //
// //     private takeBatches(): PendingBatch[] {
// //         const batches: PendingBatch[] = [];
// //         let count = 0;
// //         while (this.queue.length > 0) {
// //             const next = this.queue[0];
// //             if (count > 0 && count + next.entries.length > TARGET_FLUSH_SIZE) break;
// //             this.queue.shift();
// //             batches.push(next);
// //             count += next.entries.length;
// //         }
// //         if (batches.length === 0 && this.queue.length > 0) {
// //             batches.push(this.queue.shift()!);
// //         }
// //         return batches;
// //     }
// //
// //     private async persist(batches: PendingBatch[]) {
// //         const entries = batches.flatMap((b) => b.entries);
// //         try {
// //             await persistEntries(entries);
// //             batches.forEach((b) => b.resolve());
// //         } catch (error) {
// //             console.error('Error persisting log batch:', error);
// //             batches.forEach((b) => b.reject(error));
// //         }
// //     }
// // }
// //
// // const batcher = new IngestBatcher();
// //
// // export async function ingestLogsHandler(req: Request, res: Response) {
// //     const rawLogs = Array.isArray(req.body) ? req.body : req.body?.logs;
// //
// //     if (!Array.isArray(rawLogs)) {
// //         return res.status(400).json({
// //             error: 'Invalid request body. Expected array or { logs: [...] }',
// //         });
// //     }
// //
// //     const rejected: Array<{ index: number; reason: string }> = [];
// //     const validLogsToInsert: LogItem[] = [];
// //
// //     for (let index = 0; index < rawLogs.length; index++) {
// //         const item = rawLogs[index];
// //         const result = LogItemSchema.safeParse(item);
// //         if (result.success) {
// //             validLogsToInsert.push(result.data);
// //         } else {
// //             const errorMessage = result.error.issues
// //                 .map((e) => `${e.path.join('.')}: ${e.message}`)
// //                 .join(', ');
// //             rejected.push({ index, reason: errorMessage });
// //         }
// //     }
// //
// //     // if (validLogsToInsert.length === 0) {
// //     //     return res.status(rawLogs.length > 0 ? 400 : 200).json({ accepted: 0, rejected });
// //     // }
// //     if (validLogsToInsert.length === 0) {
// //         return res.status(400).json({
// //             accepted: 0,
// //             rejected
// //         });
// //     }
// //
// //     try {
// //         await batcher.save(validLogsToInsert);
// //         return res.status(200).json({
// //             accepted: validLogsToInsert.length,
// //             rejected,
// //         });
// //     } catch (error) {
// //         return res.status(503).json({ error: 'Failed to persist logs, try again shortly' });
// //     }
// // }