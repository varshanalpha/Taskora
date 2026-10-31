import 'dotenv/config';
import { Inngest } from "inngest";
import prisma from "../src/prisma.js";

export const inngest = new Inngest({
    id: "Taskora",
    eventKey: process.env.INNGEST_EVENT_KEY || process.env.INGEST_EVENT_KEY,
});

/**
 * Safely extract primary email address from Clerk user payload.
 */
const extractPrimaryEmail = (data) => {
    if (!data) return null;
    if (Array.isArray(data.email_addresses) && data.email_addresses.length > 0) {
        if (data.primary_email_address_id) {
            const primary = data.email_addresses.find(
                (e) => e.id === data.primary_email_address_id
            );
            if (primary?.email_address) return primary.email_address.trim().toLowerCase();
        }
        if (data.email_addresses[0]?.email_address) {
            return data.email_addresses[0].email_address.trim().toLowerCase();
        }
    }
    return null;
};

/**
 * Safely extract user name from Clerk user payload with sensible fallbacks.
 */
const extractName = (data, fallbackEmail = "") => {
    if (!data) return "User";
    const firstName = (data.first_name || "").trim();
    const lastName = (data.last_name || "").trim();
    const fullName = `${firstName} ${lastName}`.trim();
    if (fullName) return fullName;
    if (data.username?.trim()) return data.username.trim();
    if (fallbackEmail && fallbackEmail.includes("@")) {
        return fallbackEmail.split("@")[0];
    }
    return "User";
};

/**
 * Safely extract profile image from Clerk user payload.
 */
const extractImage = (data) => {
    if (!data) return "";
    return data.image_url || data.profile_image_url || "";
};

// Sync user creation from Clerk
const syncUserCreation = inngest.createFunction(
    {
        id: "sync-user-from-clerk",
        triggers: [{ event: "clerk/user.created" }],
    },
    async ({ event, step }) => {
        const { data } = event;
        const eventName = event.name || "clerk/user.created";

        console.log(`[${eventName}][sync-user-from-clerk] Processing user creation for userId: ${data?.id}`);

        if (!data?.id) {
            console.error(`[${eventName}][sync-user-from-clerk] Operation failed: Missing user ID in event data`);
            return { success: false, reason: "missing_user_id" };
        }

        return await step.run("sync-user-to-db", async () => {
            const email = extractPrimaryEmail(data);
            if (!email) {
                console.error(`[${eventName}][sync-user-from-clerk] Operation failed: Missing email address for userId: ${data.id}`);
                throw new Error(`Cannot sync user ${data.id}: Missing primary email address`);
            }

            const name = extractName(data, email);
            const image = extractImage(data);

            // Handle potential email collision if a user already exists with this email under a different ID
            const existingByEmail = await prisma.user.findUnique({
                where: { email },
            });

            if (existingByEmail && existingByEmail.id !== data.id) {
                console.warn(
                    `[${eventName}][sync-user-from-clerk] User with email already exists with id ${existingByEmail.id}. Updating record...`
                );
                const updated = await prisma.user.update({
                    where: { email },
                    data: { name, image },
                });
                console.log(`[${eventName}][sync-user-from-clerk] Database synchronization succeeded (updated by email).`);
                return { success: true, userId: updated.id, action: "updated_existing_email" };
            }

            // Idempotent upsert by Clerk ID to safely handle duplicate events and retries
            const user = await prisma.user.upsert({
                where: { id: data.id },
                create: {
                    id: data.id,
                    email,
                    name,
                    image,
                },
                update: {
                    email,
                    name,
                    image,
                },
            });

            console.log(`[${eventName}][sync-user-from-clerk] Database synchronization succeeded for userId: ${user.id}`);
            return { success: true, userId: user.id, action: "upserted" };
        });
    }
);

// Sync user deletion from Clerk
const syncUserDeletion = inngest.createFunction(
    {
        id: "delete-user-with-clerk",
        triggers: [{ event: "clerk/user.deleted" }],
    },
    async ({ event, step }) => {
        const { data } = event;
        const eventName = event.name || "clerk/user.deleted";

        console.log(`[${eventName}][delete-user-with-clerk] Processing user deletion for userId: ${data?.id}`);

        if (!data?.id) {
            console.error(`[${eventName}][delete-user-with-clerk] Operation failed: Missing user ID in event data`);
            return { success: false, reason: "missing_user_id" };
        }

        return await step.run("delete-user-from-db", async () => {
            const existingUser = await prisma.user.findUnique({
                where: { id: data.id },
            });

            if (!existingUser) {
                console.log(`[${eventName}][delete-user-with-clerk] User ${data.id} does not exist in DB (already deleted).`);
                return { success: true, userId: data.id, action: "already_deleted" };
            }

            // Cascading deletion handles all related user records via Prisma schema
            await prisma.user.delete({
                where: { id: data.id },
            });

            console.log(`[${eventName}][delete-user-with-clerk] Database synchronization succeeded: Deleted user ${data.id}`);
            return { success: true, userId: data.id, action: "deleted" };
        });
    }
);

// Sync user updates from Clerk
const syncUserUpdation = inngest.createFunction(
    {
        id: "update-user-from-clerk",
        triggers: [{ event: "clerk/user.updated" }],
    },
    async ({ event, step }) => {
        const { data } = event;
        const eventName = event.name || "clerk/user.updated";

        console.log(`[${eventName}][update-user-from-clerk] Processing user update for userId: ${data?.id}`);

        if (!data?.id) {
            console.error(`[${eventName}][update-user-from-clerk] Operation failed: Missing user ID in event data`);
            return { success: false, reason: "missing_user_id" };
        }

        return await step.run("update-user-in-db", async () => {
            const existingUser = await prisma.user.findUnique({
                where: { id: data.id },
            });

            const email = extractPrimaryEmail(data);
            const image = extractImage(data);

            // If user does not yet exist in DB (out-of-order events), safely create the user
            if (!existingUser) {
                console.log(`[${eventName}][update-user-from-clerk] User ${data.id} not found in DB. Creating user...`);
                if (!email) {
                    console.error(`[${eventName}][update-user-from-clerk] Operation failed: Missing email for userId: ${data.id}`);
                    throw new Error(`Cannot create missing user ${data.id}: Missing primary email address`);
                }
                const name = extractName(data, email);
                const user = await prisma.user.create({
                    data: {
                        id: data.id,
                        email,
                        name,
                        image,
                    },
                });
                console.log(`[${eventName}][update-user-from-clerk] Database synchronization succeeded (created new user): ${user.id}`);
                return { success: true, userId: user.id, action: "created" };
            }

            // User exists: avoid overwriting valid fields with empty/undefined values
            const updatePayload = {};

            if (email && email !== existingUser.email) {
                updatePayload.email = email;
            }

            const firstName = (data.first_name || "").trim();
            const lastName = (data.last_name || "").trim();
            const newName = `${firstName} ${lastName}`.trim() || data.username?.trim();
            if (newName && newName !== existingUser.name) {
                updatePayload.name = newName;
            }

            if (image && image !== existingUser.image) {
                updatePayload.image = image;
            }

            if (Object.keys(updatePayload).length > 0) {
                const updated = await prisma.user.update({
                    where: { id: data.id },
                    data: updatePayload,
                });
                console.log(`[${eventName}][update-user-from-clerk] Database synchronization succeeded for userId: ${updated.id}`);
                return { success: true, userId: updated.id, action: "updated" };
            }

            console.log(`[${eventName}][update-user-from-clerk] Database synchronization: No field changes needed for userId: ${data.id}`);
            return { success: true, userId: data.id, action: "noop" };
        });
    }
);
// Inngest Function to save workspace data to a database
const syncWorkspaceCreation = inngest.createFunction(
    {
        id: 'sync-workspace-from-clerk',
        triggers: [{ event: 'clerk/organization.created' }]
    },
    async ({ event }) => {
        const { data } = event;

        await prisma.workspace.create({
            data: {
                id: data.id,
                name: data.name,
                slug: data.slug,
                ownerId: data.created_by,
                image_url: data.image_url,
            }

        })
        // Add creator as ADMIN member
        await prisma.workspaceMember.create({
            data: {
                userId: data.created_by,
                workspaceId: data.id,
                role: "ADMIN"
            }
        })

    }
)
// Inngest Function to update workspace data in database
const syncWorkspaceUpdation = inngest.createFunction(
    {
        id: 'update-workspace-from-clerk',
        triggers: [{ event: 'clerk/organization.updated' }]
    },
    async ({ event }) => {
        const { data } = event;

        await prisma.workspace.update({
            where: {
                id: data.id
            },
            data: {
                name: data.name,
                slug: data.slug,
                image_url: data.image_url,
            }
        });
    }
)
// Inngest Function to save workspace member data to a database
const syncWorkspaceMemberCreation = inngest.createFunction(
    {
        id: 'sync-workspace-member-from-clerk',
        triggers: [{ event: 'clerk/organizationInvitation.accepted' }]
    },
    async ({ event }) => {
        const { data } = event;

        await prisma.workspaceMember.create({
            data: {
                userId: data.user_id,
                workspaceId: data.organization_id,
                role: String(data.role_name).toUpperCase(),
            }
        });
    }
)
const syncWorkspaceDeletion = inngest.createFunction(
    {
        id: 'delete-workspace-with-clerk',
        triggers: [{ event: 'clerk/organization.deleted' }]

    },
    async ({ event }) => {
        const { data } = event;

        await prisma.workspace.delete({
            where: {
                id: data.id
            }
        });
    }
)


// Export Inngest functions
export const functions = [
    syncUserCreation,
    syncUserDeletion,
    syncUserUpdation,
    syncWorkspaceCreation,
    syncWorkspaceDeletion,
    syncWorkspaceUpdation,
    syncWorkspaceMemberCreation,
];
