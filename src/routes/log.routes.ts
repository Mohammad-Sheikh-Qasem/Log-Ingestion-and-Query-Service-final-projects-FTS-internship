import { Router } from 'express';
import { ingestLogsHandler } from '../controllers/log.controller.js';
import { queryLogsHandler } from '../controllers/query.controller.js';
import { aggregateLogsHandler } from '../controllers/aggregate.controller.js';
import { cleanupOldLogs } from '../services/retention.service.js';
