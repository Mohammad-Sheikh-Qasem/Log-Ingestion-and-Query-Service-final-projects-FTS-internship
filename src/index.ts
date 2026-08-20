import express from 'express';
import dotenv from 'dotenv';
import { pool } from './db/index.js';
import { runMigrations } from './db/migrate.js';
import logRoutes from './routes/log.routes.js';
import { startRetentionScheduler } from './services/retention.service.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());


app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
    if (err instanceof SyntaxError && 'body' in err) {
        return res.status(400).json({ error: 'Malformed JSON in request body' });
    }
    next(err);
});


// Routes
app.use(logRoutes);

app.get('/health', async (req, res) => {
    try {
        await pool.query('SELECT 1');
        res.status(200).json({ status: 'healthy', database: 'connected' });
    } catch (error) {
        res.status(500).json({ status: 'unhealthy', database: 'disconnected' });
    }
});

async function startServer() {
    try {
        await runMigrations();

        startRetentionScheduler();

        app.listen(PORT, () => {
            console.log(`Server is running on port ${PORT}`);
        });
    } catch (error) {
        console.error('Failed to start server:', error);
        process.exit(1);
    }
}

startServer();