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
exports.cleanupOldLogs = cleanupOldLogs;
exports.startRetentionScheduler = startRetentionScheduler;
const index_js_1 = require("../db/index.js");
const RETENTION_DAYS = parseInt(process.env.RETENTION_DAYS || '30', 10);
const BATCH_SIZE = 5000;
function cleanupOldLogs() {
    return __awaiter(this, void 0, void 0, function* () {
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
                const result = yield index_js_1.pool.query(query, [cutoffDate, BATCH_SIZE]);
                deletedInBatch = result.rowCount || 0;
                totalDeleted += deletedInBatch;
            } while (deletedInBatch >= BATCH_SIZE);
            if (totalDeleted > 0) {
                console.log(`[Retention] Cleaned up ${totalDeleted} logs older than ${RETENTION_DAYS} days.`);
            }
        }
        catch (error) {
            console.error('[Retention] Error running cleanup job:', error);
        }
        return totalDeleted;
    });
}
function startRetentionScheduler(intervalMs = 60 * 60 * 1000) {
    console.log(`[Retention] Scheduler started. Checking every ${intervalMs / 1000 / 60} minutes.`);
    cleanupOldLogs();
    setInterval(() => {
        cleanupOldLogs();
    }, intervalMs);
}
