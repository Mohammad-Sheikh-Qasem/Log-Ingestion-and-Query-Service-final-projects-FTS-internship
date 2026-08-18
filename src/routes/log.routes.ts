import { Router } from 'express';
import { ingestLogsHandler } from '../controllers/log.controller.js';
import { queryLogsHandler } from '../controllers/query.controller.js';
import { aggregateLogsHandler } from '../controllers/aggregate.controller.js';
import { cleanupOldLogs } from '../services/index.js';


const router = Router();

router.post('/logs', ingestLogsHandler);

export default router;