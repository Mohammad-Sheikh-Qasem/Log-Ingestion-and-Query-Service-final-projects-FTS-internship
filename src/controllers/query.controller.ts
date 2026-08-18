import { Request, Response } from 'express';
import { pool } from '../db/index.js';
import { decodeCursor, encodeCursor } from '../utils/cursor.js';

const VALID_LEVELS = ['debug', 'info', 'warn', 'error'];

function isValidIsoTimestamp(val: string): boolean {
    const d = new Date(val);
    return !isNaN(d.getTime());
}