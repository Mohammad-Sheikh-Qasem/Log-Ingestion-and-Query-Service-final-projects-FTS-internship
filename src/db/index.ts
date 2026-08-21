import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
    throw new Error('DATABASE_URL is not defined in .env file');
}

const { Pool } = pg;

export const pool = new Pool({
    connectionString,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

// بدون هالمعالج، أي خطأ اتصال خلفي (انقطاع مؤقت بقاعدة البيانات، إعادة تشغيل)
// بيخلي Node ينهي العملية بالكامل بدل ما يتعافى — فحص الصحة كان يرجع 503 مؤقتًا
// ويستمر، بدل ما السيرفس كله يتوقف
pool.on('error', (err) => {
    console.error('Unexpected error on idle client', err);
});


// التعديل المرفوع


// import pg from 'pg';
// import dotenv from 'dotenv';
//
// dotenv.config();
//
// const connectionString = process.env.DATABASE_URL;
//
// if(!connectionString){
//     throw new Error("DATABASE_URL in not defined in .env file");
// }
//
// const { Pool } = pg;
//
// export const pool = new Pool({
//     connectionString,
//     max: 20,
//     idleTimeoutMillis: 30000,
//     connectionTimeoutMillis: 5000,
// });
//
