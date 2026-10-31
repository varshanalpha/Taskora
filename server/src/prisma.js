import 'dotenv/config';
import pg from 'pg';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from './generated/prisma/client.js';

const { Pool } = pg;

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
    console.error('[Prisma] Missing DATABASE_URL environment variable.');
}

// Reusable PostgreSQL connection pool for Prisma adapter
const pool = new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

const adapter = new PrismaPg(pool);

export const prisma = new PrismaClient({ adapter });

export { pool };
export default prisma;

