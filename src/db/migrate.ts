import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const connectionString = process.env.DATABASE_URL;

if(!connectionString){
    throw new Error("DATABASE_URL in not defined in .env file");
}

const { Pool } = pg;

export const pool = new Pool({
    connectionString,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

