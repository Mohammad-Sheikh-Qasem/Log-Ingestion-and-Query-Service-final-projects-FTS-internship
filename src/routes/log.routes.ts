import { Router } from 'express';
import { ingestLogsHandler } from '../controllers/log.controller.js';
import { queryLogsHandler } from '../controllers/query.controller.js';
import { aggregateLogsHandler } from '../controllers/aggregate.controller.js';
import { cleanupOldLogs } from '../services/index.js';


const router = Router();

router.post('/logs', ingestLogsHandler);
router.get('/logs', queryLogsHandler);
router.get('/logs/aggregate', aggregateLogsHandler);

router.post('/admin/retention/run', async (req, res) => {
    const deletedCount = await cleanupOldLogs();
    res.status(200).json({ status: 'success', deletedCount });
});



export default router;