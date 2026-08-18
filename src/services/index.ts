import { pool } from '../db/index';

const RETENTION_DAYS = parseInt(process.env.RETENTION_DAYS || '30', 10);
const BATCH_SIZE = 5000;

export async function cleanupOldLogs(): Promise<number> {
    let totalDeleted = 0;
    let deletedIntBatch = 0;

    try {
        const cutoffDate = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000).toISOString();

    }catch(error){

    }

    return 0;

}