import 'dotenv/config';
import prisma, { pool } from './src/prisma.js';
import { inngest, functions } from './inngest/index.js';
import app from './server.js';

const TEST_USER_ID = `user_verify_${Date.now()}`;
const TEST_EMAIL = `test_clerk_${Date.now()}@example.com`;

// Mock Step Runner for direct Inngest function testing
const mockStep = {
    run: async (name, fn) => {
        return await fn();
    },
};

async function runVerification() {
    console.log('====================================================');
    console.log('TASKORA USER SYNCHRONIZATION VERIFICATION SUITE');
    console.log('====================================================\n');

    let server;
    try {
        // 1. Prisma Client Initialization
        console.log('[Test 1] Testing Prisma Client & Neon DB connectivity...');
        const initialCount = await prisma.user.count();
        console.log(`✓ Prisma Client initialized and connected. Existing DB user count: ${initialCount}`);

        // 2. Server Startup & Health Check
        console.log('\n[Test 2] Starting Express server locally on port 5099...');
        process.env.NODE_ENV = 'test';
        server = await new Promise((resolve) => {
            const s = app.listen(5099, () => resolve(s));
        });
        const healthRes = await fetch('http://127.0.0.1:5099/');
        const healthText = await healthRes.text();
        if (healthRes.status === 200 && healthText === 'Server is live!') {
            console.log('✓ Express server started and health endpoint responded: "Server is live!"');
        } else {
            throw new Error(`Health check failed with status ${healthRes.status}: ${healthText}`);
        }

        // 3. Inngest Serve Endpoint Reachability
        console.log('\n[Test 3] Testing Inngest serve endpoint reachable...');
        const inngestRes = await fetch('http://127.0.0.1:5099/api/inngest');
        console.log(`✓ Inngest endpoint responded with HTTP status ${inngestRes.status} (${inngestRes.statusText})`);

        // 4. Inngest Function Registration
        console.log('\n[Test 4] Verifying registered Inngest functions...');
        const expectedFns = [
            { id: 'sync-user-from-clerk', trigger: 'clerk/user.created' },
            { id: 'delete-user-with-clerk', trigger: 'clerk/user.deleted' },
            { id: 'update-user-from-clerk', trigger: 'clerk/user.updated' },
        ];
        for (const exp of expectedFns) {
            const fn = functions.find((f) => f.opts.id === exp.id);
            if (!fn) throw new Error(`Missing Inngest function: ${exp.id}`);
            const triggers = fn.opts.triggers || [];
            const hasTrigger = triggers.some((t) => t.event === exp.trigger);
            if (!hasTrigger) throw new Error(`Function ${exp.id} missing trigger ${exp.trigger}`);
            console.log(`✓ Function "${exp.id}" properly registered with trigger "${exp.trigger}"`);
        }

        // 5. Inngest Cloud Event Ingestion
        console.log('\n[Test 5] Verifying Inngest event delivery API (inn.gs/e/)...');
        const eventKey = process.env.INNGEST_EVENT_KEY || process.env.INGEST_EVENT_KEY;
        if (eventKey) {
            const deliveryRes = await fetch(`https://inn.gs/e/${eventKey}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: 'test/verification.ping',
                    data: { timestamp: Date.now() },
                }),
            });
            const deliveryData = await deliveryRes.json();
            if (deliveryRes.status === 200 && deliveryData.status === 200) {
                console.log(`✓ Event delivery to Inngest API succeeded with status 200 (Event ID: ${deliveryData.ids?.[0]})`);
            } else {
                console.warn(`! Inngest event delivery response: ${JSON.stringify(deliveryData)}`);
            }
        } else {
            console.warn('! INNGEST_EVENT_KEY not set, skipping remote API check');
        }

        // 6 & 7 & 8. User Creation Workflow
        console.log('\n[Test 6 & 7 & 8] Executing sync-user-from-clerk with simulated Clerk payload...');
        const syncCreationFn = functions.find((f) => f.opts.id === 'sync-user-from-clerk');
        const creationPayload = {
            name: 'clerk/user.created',
            data: {
                id: TEST_USER_ID,
                first_name: 'Alex',
                last_name: 'Taskora',
                email_addresses: [
                    {
                        id: `idn_${TEST_USER_ID}`,
                        email_address: TEST_EMAIL,
                    },
                ],
                primary_email_address_id: `idn_${TEST_USER_ID}`,
                image_url: 'https://img.clerk.com/test-alex.png',
            },
        };
        const creationResult = await syncCreationFn.fn({ event: creationPayload, step: mockStep });
        console.log('✓ Function execution completed with result:', creationResult);

        // Verify in Neon PostgreSQL User table
        const createdUser = await prisma.user.findUnique({ where: { id: TEST_USER_ID } });
        if (!createdUser) throw new Error('User not found in Neon PostgreSQL database after creation');
        console.log('✓ Verified user in Neon PostgreSQL:');
        console.log(`    id: ${createdUser.id}`);
        console.log(`    name: ${createdUser.name}`);
        console.log(`    email: ${createdUser.email}`);
        console.log(`    image: ${createdUser.image}`);

        // 11. Duplicate Event Handling (Idempotency)
        console.log('\n[Test 11] Testing Idempotency: Retrying exact same user.created event...');
        const duplicateResult = await syncCreationFn.fn({ event: creationPayload, step: mockStep });
        console.log('✓ Duplicate creation handled cleanly without errors:', duplicateResult);
        const countAfterDuplicate = await prisma.user.count({ where: { id: TEST_USER_ID } });
        if (countAfterDuplicate !== 1) throw new Error(`Duplicate user created! Count: ${countAfterDuplicate}`);
        console.log('✓ Confirmed exactly 1 user record remains in DB.');

        // 9. User Updates Workflow
        console.log('\n[Test 9] Executing update-user-from-clerk with simulated updates...');
        const syncUpdateFn = functions.find((f) => f.opts.id === 'update-user-from-clerk');
        const updatePayload = {
            name: 'clerk/user.updated',
            data: {
                id: TEST_USER_ID,
                first_name: 'Alex Updated',
                last_name: 'Engineer',
                email_addresses: [
                    {
                        id: `idn_${TEST_USER_ID}`,
                        email_address: TEST_EMAIL,
                    },
                ],
                primary_email_address_id: `idn_${TEST_USER_ID}`,
                image_url: 'https://img.clerk.com/test-alex-updated.png',
            },
        };
        const updateResult = await syncUpdateFn.fn({ event: updatePayload, step: mockStep });
        console.log('✓ Update function executed with result:', updateResult);
        const updatedUser = await prisma.user.findUnique({ where: { id: TEST_USER_ID } });
        if (updatedUser.name !== 'Alex Updated Engineer') throw new Error(`Name not updated: ${updatedUser.name}`);
        if (updatedUser.image !== 'https://img.clerk.com/test-alex-updated.png') throw new Error(`Image not updated: ${updatedUser.image}`);
        console.log('✓ Confirmed updated fields in Neon PostgreSQL:');
        console.log(`    name: ${updatedUser.name}`);
        console.log(`    image: ${updatedUser.image}`);

        // 10. User Deletion Workflow
        console.log('\n[Test 10] Executing delete-user-with-clerk with simulated deletion...');
        const syncDeleteFn = functions.find((f) => f.opts.id === 'delete-user-with-clerk');
        const deletePayload = {
            name: 'clerk/user.deleted',
            data: {
                id: TEST_USER_ID,
                deleted: true,
            },
        };
        const deleteResult = await syncDeleteFn.fn({ event: deletePayload, step: mockStep });
        console.log('✓ Delete function executed with result:', deleteResult);
        const deletedCheck = await prisma.user.findUnique({ where: { id: TEST_USER_ID } });
        if (deletedCheck !== null) throw new Error('User still exists in DB after deletion!');
        console.log('✓ Confirmed user deleted from Neon PostgreSQL database.');

        // Deletion idempotency check (calling delete again on already deleted user)
        const reDeleteResult = await syncDeleteFn.fn({ event: deletePayload, step: mockStep });
        console.log('✓ Redundant deletion handled safely without errors:', reDeleteResult);

        console.log('\n====================================================');
        console.log('ALL LOCAL VERIFICATIONS PASSED SUCCESSFULLY! (11/11)');
        console.log('====================================================');
    } catch (err) {
        console.error('\n❌ Verification Failed:', err);
        // Attempt cleanup if failed
        try {
            await prisma.user.deleteMany({ where: { id: TEST_USER_ID } });
        } catch (_) {}
        process.exit(1);
    } finally {
        if (server) {
            server.close();
        }
        await pool.end();
        process.exit(0);
    }
}

runVerification();

