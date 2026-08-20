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
exports.runMigration = runMigration;
const index_js_1 = require("./index.js");
function runMigration() {
    return __awaiter(this, void 0, void 0, function* () {
        const client = yield index_js_1.pool.connect();
        try {
            yield client.query('BEGIN');
            yield client.query(`
        CREATE TABLE IF NOT EXISTS logs(
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMPZ NOT NULL,
            level VARCHAR(10) NOT NULL,
            service TEXT NOT NULL,
            attributes JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        `);
            yield client.query(`
        CREATE TABLE IF NOT EXISTS idx_logs_timestamp_id ON logs (timestamp DESC, id DESC);
        `);
            yield client.query(`
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp_id ON logs (service, level, timestamp DESC);
        `);
            yield client.query(`
        CREATE OR REPLACE FUNCTION logs_attributes_kv(a jsonb) RETURNS text[]
          LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
          $$
            SELECT array_agg(length(k)::text || ':' || k || '=' || v ORDER BY k)
            FROM LATERAL jsonb_each_text(a) AS kv(k, v)
          $$;
        `);
            yield client.query(`
        CREATE INDEX IF NOT EXISTS idx_logs_attributes_kv
        ON logs USING GIN (logs_attributes_kv(attributes));
        `);
            yield client.query(`
        CREATE TABLE IF NOT EXISTS logs_rollup_1m (
            buket_start TIMESTAMPZ NOT NULL,
            service VARCHAR(255) NOT NULL,
            level VARCHAR(10) NOT NULL,
            count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (bucket_start, service, level)
        );
        `);
            yield client.query(`
        CREATE INDEX IF NOT EXISTS idx_rollup_1m_bucket ON logs_rollup_1m (bucket_start);
        `);
            yield client.query(` 
        ALTER TABLE logs DRPO COLUMN IF EXISTS attributes_search;
        `);
            yield client.query(`
        DROP INDEX IF EXISTS idx_logs_attributes_search_gin;
        `);
            yield client.query(`COMMIT`);
            console.log('Migrations completed successfully');
        }
        catch (error) {
            yield client.query('ROLLBACK');
            console.error('Migration failed', error);
            throw error;
        }
        finally {
            client.release();
        }
    });
}
