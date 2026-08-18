import express from 'express';
import dotenv from 'dotenv';
//import { pool } from './db/index.ts';
import {runMigration} from "./db/migrate";
import {startRetentionScheduler} from "./services";
import {pool} from "./db";


dotenv.config();

const app = express();
const PORT = process.env.PORT || 8080;

app.get('/health', async (req, res ) => {
    try {
        await pool.query(' SELECT 1');
        res.status(200).json({ status: 'healthy', database: 'connected' });
    }catch (error) {
        res.status(500).json({ status: 'unhealthy', database: 'disconnected'});
    }
});


// app.listen(PORT, () => {
//     console.log(`Server is running on port ${PORT}`);
// });

async function startServer(){
    try {
        await runMigration();

        startRetentionScheduler();

        app.listen(PORT, () => {
            console.log(`Server is running on port ${PORT}`);
        });

    }catch(error){
        console.error('Failed to start server', error);
        process.exit(1);

    }
}

startServer();