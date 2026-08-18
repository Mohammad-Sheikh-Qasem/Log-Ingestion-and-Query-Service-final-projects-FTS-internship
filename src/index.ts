import express from 'express';
import dotenv from 'dotenv';
//import { pool } from './db/index.ts';
import {runMigration} from "./db/migrate";
import {startRetentionScheduler} from "./services";


dotenv.config();

const app = express();
const PORT = process.env.PORT || 8080;


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