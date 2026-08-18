import { pool} from './index.js';


export async function runMigration(){
    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        await client.query(`
        CREATE TABLE IF NOT EXISTS logs(
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMPZ NOT NULL,
            level VARCHAR(10) NOT NULL,
            service TEXT NOT NULL,
            attributes JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        `);

        await client.query(`
        CREATE TABLE IF NOT EXISTS idx_logs_timestamp_id ON logs (timestamp DESC, id DESC);
        `);

        await client.query(`
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp_id ON logs (service, level, timestamp DESC);
        `);


        await client.query(`
        CREATE OR REPLACE FUNCTION logs_attributes_kv(a jsonb) RETURNS text[]
          LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
          $$
            SELECT array_agg(length(k)::text || ':' || k || '=' || v ORDER BY k)
            FROM LATERAL jsonb_each_text(a) AS kv(k, v)
          $$;
        `);


        await client.query(`
        CREATE INDEX IF NOT EXISTS idx_logs_attributes_kv
        ON logs USING GIN (logs_attributes_kv(attributes));
        `);

        await client.query(`
        CREATE TABLE IF NOT EXISTS logs_rollup_1m (
            buket_start TIMESTAMPZ NOT NULL,
            service VARCHAR(255) NOT NULL,
            level VARCHAR(10) NOT NULL,
            count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (bucket_start, service, level)
        );
        `);

        await client.query(`
        CREATE INDEX IF NOT EXISTS idx_rollup_1m_bucket ON logs_rollup_1m (bucket_start);
        `);

        await client.query(` 
        ALTER TABLE logs DRPO COLUMN IF EXISTS attributes_search;
        `);

        await client.query(`
        DROP INDEX IF EXISTS idx_logs_attributes_search_gin;
        `);



    }catch(error){

    }
}