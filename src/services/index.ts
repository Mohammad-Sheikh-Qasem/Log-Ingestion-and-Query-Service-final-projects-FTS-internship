import { pool } from '../db/index.js';

const RETENTION_DAYS = parseInt(process.env.RETENTION_DAYS || '30', 10);
const BATCH_SIZE = 5000;

export async function cleanupOldLogs(): Promise<number> {
    let totalDeleted = 0;
    let deletedInBatch = 0;

    try {
        const cutoffDate = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000).toISOString();

        do {
            const query = `
        WITH rows_to_delete AS (
          SELECT id FROM logs
          WHERE timestamp < $1
          LIMIT $2
        )
        DELETE FROM logs
        WHERE id IN (SELECT id FROM rows_to_delete);
      `;

            const result = await pool.query(query, [cutoffDate, BATCH_SIZE]);
            deletedInBatch = result.rowCount || 0;
            totalDeleted += deletedInBatch;

        } while (deletedInBatch >= BATCH_SIZE);

        if (totalDeleted > 0) {
            console.log(`[Retention] Cleaned up ${totalDeleted} logs older than ${RETENTION_DAYS} days.`);
        }
    } catch (error) {
        console.error('[Retention] Error running cleanup job:', error);
    }

    return totalDeleted;
}

export function startRetentionScheduler(intervalMs = 60 * 60 * 1000) {
    console.log(`[Retention] Scheduler started. Checking every ${intervalMs / 1000 / 60} minutes.`);

    cleanupOldLogs();

    setInterval(() => {
        cleanupOldLogs();
    }, intervalMs);
}