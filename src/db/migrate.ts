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

    }catch(error){

    }
}