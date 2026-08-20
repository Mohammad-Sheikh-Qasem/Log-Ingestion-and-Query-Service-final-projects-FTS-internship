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
exports.aggregateLogsHandler = aggregateLogsHandler;
const index_js_1 = require("../db/index.js");
const VALID_LEVELS = ['debug', 'info', 'warn', 'error'];
const BUCKET_INTERVALS = {
    '1m': '1 minute',
    '5m': '5 minutes',
    '1h': '1 hour',
    '1d': '1 day',
};
function isValidIsoTimestamp(val) {
    const d = new Date(val);
    return !isNaN(d.getTime());
}
function encodeAttributeKv(key, value) {
    return `${key.length}:${key}=${value}`;
}
function aggregateLogsHandler(req, res) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const { bucket, since, until, service, level, q, group_by } = req.query;
            if (typeof bucket !== 'string' || !BUCKET_INTERVALS[bucket]) {
                return res.status(400).json({ error: 'Invalid bucket value. Allowed values: 1m, 5m, 1h, 1d' });
            }
            if (typeof since !== 'string' || !isValidIsoTimestamp(since)) {
                return res.status(400).json({ error: 'since is required and must be a valid ISO 8601 timestamp' });
            }
            if (typeof until !== 'string' || !isValidIsoTimestamp(until)) {
                return res.status(400).json({ error: 'until is required and must be a valid ISO 8601 timestamp' });
            }
            if (new Date(until) < new Date(since)) {
                return res.status(400).json({ error: 'until must not be earlier than since' });
            }
            if (level !== undefined && (typeof level !== 'string' || !VALID_LEVELS.includes(level))) {
                return res.status(400).json({ error: 'Invalid level. Must be one of: debug, info, warn, error' });
            }
            if (group_by !== undefined && group_by !== 'service' && group_by !== 'level') {
                return res.status(400).json({ error: "group_by must be 'service' or 'level'" });
            }
            const interval = BUCKET_INTERVALS[bucket];
            const groupCol = group_by === 'service' ? 'service' : group_by === 'level' ? 'level' : null;
            const attributeEntries = [];
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
            const needsRawTable = (q && typeof q === 'string') || attributeEntries.length > 0;
            let rows;
            if (!needsRawTable) {
                const conditions = [`bucket_start >= $1`, `bucket_start < $2`];
                const values = [since, until];
                let paramIndex = 3;
                if (service && typeof service === 'string') {
                    conditions.push(`service = $${paramIndex++}`);
                    values.push(service);
                }
                if (typeof level === 'string') {
                    conditions.push(`level = $${paramIndex++}`);
                    values.push(level);
                }
                const whereClause = `WHERE ${conditions.join(' AND ')}`;
                const selectGroup = groupCol ? `, ${groupCol} AS group_value` : `, NULL AS group_value`;
                const groupByClause = groupCol ? `, ${groupCol}` : '';
                const query = `
        SELECT 
          date_bin($${paramIndex}::interval, bucket_start, '2000-01-01 00:00:00Z'::timestamptz) AS bucket_start,
          SUM(count)::bigint AS count
          ${selectGroup}
        FROM logs_rollup_1m
        ${whereClause}
        GROUP BY 1 ${groupByClause}
        ORDER BY 1 ASC;
      `;
                values.push(interval);
                const result = yield index_js_1.pool.query(query, values);
                rows = result.rows;
            }
            else {
                // مسار بديل: فلاتر q أو attr.<key> — نعزل المرشحين عبر GIN بواسطة CTE مجمّد أولًا
                const values = [];
                let paramIndex = 1;
                const attributeClauses = attributeEntries.map(([key, value]) => {
                    const kv = encodeAttributeKv(key, value);
                    values.push(kv);
                    return `logs_attributes_kv(attributes) @> ARRAY[$${paramIndex++}]::text[]`;
                });
                const restConditions = [`timestamp >= $${paramIndex++}`, `timestamp < $${paramIndex++}`];
                values.push(since, until);
                if (service && typeof service === 'string') {
                    restConditions.push(`service = $${paramIndex++}`);
                    values.push(service);
                }
                if (typeof level === 'string') {
                    restConditions.push(`level = $${paramIndex++}`);
                    values.push(level);
                }
                if (q && typeof q === 'string') {
                    restConditions.push(`message ILIKE $${paramIndex++}`);
                    values.push(`%${q}%`);
                }
                const selectGroup = groupCol ? `, ${groupCol} AS group_value` : `, NULL AS group_value`;
                const groupByClause = groupCol ? `, ${groupCol}` : '';
                const restWhere = `WHERE ${restConditions.join(' AND ')}`;
                let query;
                if (attributeClauses.length > 0) {
                    query = `
          WITH attribute_matches AS MATERIALIZED (
            SELECT timestamp, service, level
            FROM logs
            WHERE ${attributeClauses.join(' AND ')}
          )
          SELECT 
            date_bin($${paramIndex}::interval, timestamp, '2000-01-01 00:00:00Z'::timestamptz) AS bucket_start,
            COUNT(*)::bigint AS count
            ${selectGroup}
          FROM attribute_matches
          ${restWhere}
          GROUP BY 1 ${groupByClause}
          ORDER BY 1 ASC;
        `;
                }
                else {
                    query = `
          SELECT 
            date_bin($${paramIndex}::interval, timestamp, '2000-01-01 00:00:00Z'::timestamptz) AS bucket_start,
            COUNT(*)::bigint AS count
            ${selectGroup}
          FROM logs
          ${restWhere}
          GROUP BY 1 ${groupByClause}
          ORDER BY 1 ASC;
        `;
                }
                values.push(interval);
                const result = yield index_js_1.pool.query(query, values);
                rows = result.rows;
            }
            // res.status(200).json(
            //     rows.map((r: any) => ({
            //         bucket_start: r.bucket_start,
            //         count: Number(r.count),
            //         group: r.group_value ?? null,
            //     }))
            // );
            res.status(200).json({
                buckets: rows.map((r) => {
                    var _a;
                    return ({
                        start: r.bucket_start,
                        group: (_a = r.group_value) !== null && _a !== void 0 ? _a : null,
                        count: Number(r.count),
                    });
                }),
            });
        }
        catch (error) {
            console.error('Aggregate logs error:', error);
            res.status(500).json({ error: 'Failed to aggregate logs' });
        }
    });
}
