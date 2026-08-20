"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ingestLogsHandler = ingestLogsHandler;
const log_schema_js_1 = require("../schemas/log.schema.js");
const index_js_1 = require("../db/index.js");
const TARGET_FLUSH_SIZE = 4000;
const MAX_CONCURRENT_FLUSHES = 3;
function truncateToMinuteISO(isoTimestamp) {
    const d = new Date(isoTimestamp);
    d.setUTCSeconds(0, 0);
    return d.toISOString();
}
function buildLogsInsertParams(entries) {
    const timestamps = [];
    const levels = [];
    const services = [];
    const messages = [];
    const attributes = [];
    for (const log of entries) {
        timestamps.push(log.timestamp);
        levels.push(log.level);
        services.push(log.service);
        messages.push(log.message);
        attributes.push(JSON.stringify(log.attributes || {}));
    }
    return { timestamps, levels, services, messages, attributes };
}
function buildRollupUpsertParams(entries) {
    const map = new Map();
    for (const log of entries) {
        const bucket = truncateToMinuteISO(log.timestamp);
        const key = `${bucket}|${log.service}|${log.level}`;
        const existing = map.get(key);
        if (existing) {
            existing.count++;
        }
        else {
            map.set(key, { bucket, service: log.service, level: log.level, count: 1 });
        }
    }
    const rows = Array.from(map.values()).sort((a, b) => a.bucket.localeCompare(b.bucket) ||
        a.service.localeCompare(b.service) ||
        a.level.localeCompare(b.level));
    return {
        buckets: rows.map((r) => r.bucket),
        services: rows.map((r) => r.service),
        levels: rows.map((r) => r.level),
        counts: rows.map((r) => r.count),
    };
}
function persistEntries(entries) {
    return __awaiter(this, void 0, void 0, function* () {
        if (entries.length === 0)
            return;
        const client = yield index_js_1.pool.connect();
        try {
            for (let attempt = 1;; attempt++) {
                try {
                    yield client.query('BEGIN');
                    const { timestamps, levels, services, messages, attributes } = buildLogsInsertParams(entries);
                    yield client.query(`INSERT INTO logs (timestamp, level, service, message, attributes)
                     SELECT t::timestamptz, l, s, m, a::jsonb
                     FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
                     AS x(t, l, s, m, a);`, [timestamps, levels, services, messages, attributes]);
                    const { buckets, services: rServices, levels: rLevels, counts } = buildRollupUpsertParams(entries);
                    yield client.query(`INSERT INTO logs_rollup_1m (bucket_start, service, level, count)
                     SELECT b::timestamptz, s, l, c::bigint
                     FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[]) AS x(b, s, l, c)
                     ON CONFLICT (bucket_start, service, level)
                     DO UPDATE SET count = logs_rollup_1m.count + EXCLUDED.count;`, [buckets, rServices, rLevels, counts]);
                    yield client.query('COMMIT');
                    return;
                }
                catch (error) {
                    yield client.query('ROLLBACK');
                    const isDeadlock = attempt < 3 &&
                        typeof error === 'object' &&
                        error !== null &&
                        error.code === '40P01';
                    if (!isDeadlock)
                        throw error;
                }
            }
        }
        finally {
            client.release();
        }
    });
}
class IngestBatcher {
    constructor() {
        this.queue = [];
        this.activeFlushes = 0;
        this.waiters = [];
    }
    save(entries) {
        if (entries.length === 0)
            return Promise.resolve();
        return new Promise((resolve, reject) => {
            this.queue.push({ entries, resolve, reject });
            this.wakeAll();
            this.spawnFlush();
        });
    }
    wakeAll() {
        const w = this.waiters;
        this.waiters = [];
        w.forEach((fn) => fn());
    }
    spawnFlush() {
        if (this.activeFlushes >= MAX_CONCURRENT_FLUSHES || this.queue.length === 0)
            return;
        this.activeFlushes++;
        this.runFlushLoop().finally(() => {
            this.activeFlushes--;
            this.spawnFlush();
        });
    }
    runFlushLoop() {
        return __awaiter(this, void 0, void 0, function* () {
            while (this.queue.length > 0) {
                const batches = this.takeBatches();
                if (batches.length === 0)
                    break;
                yield this.persist(batches);
            }
        });
    }
    takeBatches() {
        const batches = [];
        let count = 0;
        while (this.queue.length > 0) {
            const next = this.queue[0];
            if (count > 0 && count + next.entries.length > TARGET_FLUSH_SIZE)
                break;
            this.queue.shift();
            batches.push(next);
            count += next.entries.length;
        }
        if (batches.length === 0 && this.queue.length > 0) {
            batches.push(this.queue.shift());
        }
        return batches;
    }
    persist(batches) {
        return __awaiter(this, void 0, void 0, function* () {
            const entries = batches.flatMap((b) => b.entries);
            try {
                yield persistEntries(entries);
                batches.forEach((b) => b.resolve());
            }
            catch (error) {
                console.error('Error persisting log batch:', error);
                batches.forEach((b) => b.reject(error));
            }
        });
    }
}
const batcher = new IngestBatcher();
function ingestLogsHandler(req, res) {
    return __awaiter(this, void 0, void 0, function* () {
        var _a;
        const rawLogs = Array.isArray(req.body) ? req.body : (_a = req.body) === null || _a === void 0 ? void 0 : _a.logs;
        if (!Array.isArray(rawLogs)) {
            return res.status(400).json({
                error: 'Invalid request body. Expected array or { logs: [...] }',
            });
        }
        const rejected = [];
        const validLogsToInsert = [];
        for (let index = 0; index < rawLogs.length; index++) {
            const item = rawLogs[index];
            const result = log_schema_js_1.LogItemSchema.safeParse(item);
            if (result.success) {
                validLogsToInsert.push(result.data);
            }
            else {
                const errorMessage = result.error.issues
                    .map((e) => `${e.path.join('.')}: ${e.message}`)
                    .join(', ');
                rejected.push({ index, reason: errorMessage });
            }
        }
        // if (validLogsToInsert.length === 0) {
        //     return res.status(rawLogs.length > 0 ? 400 : 200).json({ accepted: 0, rejected });
        // }
        if (validLogsToInsert.length === 0) {
            return res.status(400).json({
                accepted: 0,
                rejected
            });
        }
        try {
            yield batcher.save(validLogsToInsert);
            return res.status(200).json({
                accepted: validLogsToInsert.length,
                rejected,
            });
        }
        catch (error) {
            return res.status(503).json({ error: 'Failed to persist logs, try again shortly' });
        }
    });
}
