import express from 'express';
import 'dotenv/config';
import cors from 'cors';
import { clerkMiddleware } from '@clerk/express';
import { serve } from 'inngest/express';

import { inngest, functions } from './inngest/index.js';

const app = express();

app.use(express.json());
app.use(cors());

// Health check endpoint
app.get('/', (req, res) => res.send('Server is live!'));

// Inngest serve endpoint (handles GET introspection, POST execution, PUT sync)
app.use(
    '/api/inngest',
    serve({
        client: inngest,
        functions,
        signingKey: process.env.INNGEST_SIGNING_KEY || process.env.INGEST_SIGNING_KEY,
    })
);

// Clerk authentication middleware for protected application routes
app.use(clerkMiddleware());

const PORT = process.env.PORT || 5001;

const isMainModule = process.argv[1] && (process.argv[1].endsWith('server.js') || process.argv[1].endsWith('server'));
if (isMainModule) {
    app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}

export default app;
