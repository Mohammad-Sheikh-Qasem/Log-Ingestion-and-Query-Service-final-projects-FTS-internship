import express from 'express';
import dotenv from 'dotenv';
//import { pool } from './db/index.ts';
import {runMigration} from "./db/migrate";


dotenv.config();

const app = express();
const PORT = process.env.PORT || 8080;


app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});

// async function startServer(){
//     try {
//         await runMigration();
//
//     }catch(error){
//
//     }
// }