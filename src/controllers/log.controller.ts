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