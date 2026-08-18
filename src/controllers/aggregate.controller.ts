import { Request, Response } from 'express';
import { pool } from '../db/index.js';

const VALID_LEVELS = ['debug', 'info', 'warn', 'error'];
const BUCKET_INTERVALS: Record<string, string> = {
    '1m': '1 minute',
    '5m': '5 minutes',
    '1h': '1 hour',
    '1d': '1 day',
};