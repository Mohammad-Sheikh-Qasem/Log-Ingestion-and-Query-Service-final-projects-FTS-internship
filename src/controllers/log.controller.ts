import { Request, Response } from 'express';
import { LogItemSchema, LogItem } from '../schemas/log.schema.js';
import { pool } from '../db/index.js';