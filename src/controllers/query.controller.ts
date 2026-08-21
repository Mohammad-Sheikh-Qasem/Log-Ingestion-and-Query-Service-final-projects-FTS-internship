import { Request, Response } from 'express';
import { pool } from '../db/index.js';
import { decodeCursor, encodeCursor } from '../utils/cursor.js';

const VALID_LEVELS = ['debug', 'info', 'warn', 'error'];

function isValidIsoTimestamp(val: string): boolean {
    const d = new Date(val);
    return !isNaN(d.getTime());
}

function encodeAttributeKv(key: string, value: string): string {
    return `${key.length}:${key}=${value}`;
}

export async function queryLogsHandler(req: Request, res: Response) {
    try {
        const { service, level, since, until, q, cursor, limit: limitQuery } = req.query;

        // ---- Validation ----
        if (level !== undefined && (typeof level !== 'string' || !VALID_LEVELS.includes(level))) {
            return res.status(400).json({ error: 'Invalid level. Must be one of: debug, info, warn, error' });
        }

        if (since !== undefined && (typeof since !== 'string' || !isValidIsoTimestamp(since))) {
            return res.status(400).json({ error: 'Invalid since timestamp' });
        }
        if (until !== undefined && (typeof until !== 'string' || !isValidIsoTimestamp(until))) {
            return res.status(400).json({ error: 'Invalid until timestamp' });
        }
        if (typeof since === 'string' && typeof until === 'string' && new Date(until) < new Date(since)) {
            return res.status(400).json({ error: 'until must not be earlier than since' });
        }

        let limit = 100;
        if (limitQuery !== undefined) {
            if (typeof limitQuery !== 'string' || !/^\d+$/.test(limitQuery)) {
                return res.status(400).json({ error: 'limit must be a positive integer' });
            }
            limit = parseInt(limitQuery, 10);
            if (limit < 1 || limit > 1000) {
                return res.status(400).json({ error: 'limit must be between 1 and 1000' });
            }
        }

        let decodedCursor: { timestamp: string; id: string } | null = null;
        if (cursor !== undefined) {
            if (typeof cursor !== 'string') {
                return res.status(400).json({ error: 'Invalid cursor' });
            }
            decodedCursor = decodeCursor(cursor);
            if (!decodedCursor) {
                return res.status(400).json({ error: 'Invalid or malformed cursor' });
            }
        }


        const attributeEntries: Array<[string, string]> = [];
        for (const key of Object.keys(req.query)) {
            if (key.startsWith('attr.')) {
                const attrKey = key.replace('attr.', '');
                const rawVal = req.query[key];
                if (typeof rawVal === 'string') {
                    attributeEntries.push([attrKey, rawVal]);
                }
            }
        }
        attributeEntries.sort((a, b) => a[0].localeCompare(b[0]));

        const values: any[] = [];
        let paramIndex = 1;

        const restClauses: string[] = [];
        if (service && typeof service === 'string') {
            restClauses.push(`service = $${paramIndex++}`);
            values.push(service);
        }
        if (typeof level === 'string') {
            restClauses.push(`level = $${paramIndex++}`);
            values.push(level);
        }
        if (typeof since === 'string') {
            restClauses.push(`timestamp >= $${paramIndex++}`);
            values.push(since);
        }
        if (typeof until === 'string') {
            restClauses.push(`timestamp < $${paramIndex++}`);
            values.push(until);
        }
        if (q && typeof q === 'string') {
            restClauses.push(`message ILIKE $${paramIndex++}`);
            values.push(`%${q}%`);
        }
        if (decodedCursor) {
            restClauses.push(
                `(timestamp, id) < ($${paramIndex++}::timestamptz, $${paramIndex++}::bigint)`
            );
            values.push(decodedCursor.timestamp, decodedCursor.id);
        }

        let query: string;

        if (attributeEntries.length > 0) {

            const attributeClauses = attributeEntries.map(([key, value]) => {
                const kv = encodeAttributeKv(key, value);
                values.push(kv);
                return `logs_attributes_kv(attributes) @> ARRAY[$${paramIndex++}]::text[]`;
            });

            const whereClause = restClauses.length > 0 ? `WHERE ${restClauses.join(' AND ')}` : '';

            query = `
        WITH attribute_matches AS MATERIALIZED (
          SELECT id, timestamp, level, service, message, attributes
          FROM logs
          WHERE ${attributeClauses.join(' AND ')}
        )
        SELECT id, timestamp, level, service, message, attributes
        FROM attribute_matches
        ${whereClause}
        ORDER BY timestamp DESC, id DESC
        LIMIT $${paramIndex++};
      `;
        } else {
            const whereClause = restClauses.length > 0 ? `WHERE ${restClauses.join(' AND ')}` : '';
            query = `
        SELECT id, timestamp, level, service, message, attributes
        FROM logs
        ${whereClause}
        ORDER BY timestamp DESC, id DESC
        LIMIT $${paramIndex++};
      `;
        }
        values.push(limit + 1);

        const { rows } = await pool.query(query, values);

        let nextCursor: string | null = null;
        if (rows.length > limit) {
            const lastItem = rows[limit - 1];
            rows.pop();
            nextCursor = encodeCursor({
                timestamp: lastItem.timestamp.toISOString(),
                id: lastItem.id,
            });
        }

        res.status(200).json({
            logs: rows,
            next_cursor: nextCursor,
        });
    } catch (error) {
        console.error('Query logs error:', error);
        res.status(500).json({ error: 'Failed to query logs' });
    }
}