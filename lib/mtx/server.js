const cds = require('@sap/cds');
const axios = require('axios');
const https = require("https");
const DEBUG = cds.debug('attachments');
const smClient = require('../utils/SMClient.js')
const { S3Client, paginateListObjectsV2, DeleteObjectsCommand } = require('@aws-sdk/client-s3');

const PATH = {
    SERVICE_INSTANCE: "v1/service_instances",
    SERVICE_BINDING: "v1/service_bindings",
    SERVICE_PLAN: "v1/service_plans",
    SERVICE_OFFERING: "v1/service_offerings"
};

const HTTP_METHOD = {
    POST: "post",
    GET: "get",
    DELETE: "delete"
};

const STATE = {
    SUCCEEDED: "succeeded",
    FAILED: "failed",
};

let POLL_WAIT_TIME = 5000;
const ASYNC_TIMEOUT = 5 * 60 * 1000;

async function wait(milliseconds) {
    if (milliseconds <= 0) {
        return;
    }
    await new Promise(function (resolve) {
        setTimeout(resolve, milliseconds);
    });
}

const _pollUntilDone = async (sm_url, instancePath, token) => {
    try {
        let iteration = 1;
        const startTime = Date.now();
        let isReady = false;
        while (!isReady) {
            await wait(POLL_WAIT_TIME * iteration);
            iteration++;

            const instanceStatus = await axios.get(`${sm_url}/${instancePath}`, {
                headers: { 'Accept': 'application/json', 'Authorization': `Bearer ${token}` }
            });

            if (instanceStatus.data.state === STATE.SUCCEEDED) {
                isReady = true;
                return instanceStatus;
            }

            if (Date.now() - startTime > ASYNC_TIMEOUT) {
                DEBUG?.('Timed out waiting for service instance to be ready');
            }

            if (instanceStatus.data.state === STATE.FAILED) {
                DEBUG?.('Service instance creation failed');
            }
        }
    } catch (error) {
        DEBUG?.(`Error polling for object store instance readiness: ${error.message}`);
    }
};

cds.on('listening', async () => {
    const profile = cds.env.profile;
    const objectStoreKind = cds.env.requires?.attachments?.objectStore?.kind;
    if (profile === 'mtx-sidecar') {
        const ds = await cds.connect.to("cds.xt.DeploymentService");
        if (objectStoreKind === "separate") {
            ds.after('subscribe', async (_, req) => {
                const { tenant } = req.data;
                try {
                    const serviceManagerCredentials = cds.env.requires?.serviceManager?.credentials || {};
                    const { sm_url, url, clientid, clientsecret, certificate, key } = serviceManagerCredentials;

                    smClient.validateSMCredentials({ sm_url, url, clientid, clientsecret, certificate, key });

                    const token = await smClient.fetchToken(url, clientid, clientsecret, certificate, key)

                    const offeringID = await smClient.getOfferingID(sm_url, token);

                    const planID = await smClient.getPlanID(sm_url, token, offeringID);

                    const instanceID = await smAuthClient.createObjectStoreInstance(sm_url, tenant, planID, token);
                    DEBUG?.('Object Store instance created');

                    await smClient.bindObjectStoreInstance(sm_url, tenant, instanceID, token);
                } catch (error) {
                    // eslint-disable-next-line no-console
                    console.error(`Error setting up object store for tenant - ${tenant}: ${error.message}`);
                }
            });

            ds.after('unsubscribe', async (_, req) => {
                const { tenant } = req.data;
                try {
                    const serviceManagerCredentials = cds.env.requires?.serviceManager?.credentials || {};
                    const { sm_url, url, clientid, clientsecret, certificate, key } = serviceManagerCredentials;

                    smClient.validateSMCredentials({ sm_url, url, clientid, clientsecret, certificate, key });

                    const token = await smClient.fetchToken(url, clientid, clientsecret, certificate, key);

                    const bindingID = await smClient.getBindingIdForDeletion(sm_url, tenant, token);

                    await smClient.deleteBinding(sm_url, bindingID, token);

                    const service_instance_id = await smClient.getInstanceIdForDeletion(sm_url, tenant, token);

                    await smClient.deleteObjectStoreInstance(sm_url, service_instance_id, token);
                } catch (error) {
                    // eslint-disable-next-line no-console
                    console.error(`Error deleting object store service for tenant - ${tenant}: ${error.message}`);
                }

            });
        } else if (objectStoreKind === "shared") {
            ds.after('unsubscribe', async (_, req) => {
                const { tenant } = req.data;

                const creds = cds.env.requires?.objectStore?.credentials;
                if (!creds) throw new Error("SAP Object Store instance credentials not found.");

                const client = new S3Client({
                    region: creds.region,
                    credentials: {
                        accessKeyId: creds.access_key_id,
                        secretAccessKey: creds.secret_access_key,
                    },
                });

                const bucket = creds.bucket;
                const keysToDelete = [];

                try {
                    const paginator = paginateListObjectsV2({ client }, {
                        Bucket: bucket,
                        Prefix: tenant,
                    });

                    for await (const page of paginator) {
                        page.Contents?.forEach(obj => {
                            keysToDelete.push({ Key: obj.Key });
                        });
                    }

                    if (keysToDelete.length > 0) {
                        await client.send(new DeleteObjectsCommand({
                            Bucket: bucket,
                            Delete: { Objects: keysToDelete },
                        }));
                        console.debug(`S3 objects deleted for tenant: ${tenant}`);
                    } else {
                        console.debug(`No S3 objects found for tenant: ${tenant}`);
                    }
                } catch (error) {
                    console.error(`Failed to clean up S3 objects for tenant "${tenant}": ${error.message}`);
                }
            });

        }
    }
    module.exports = cds.server;
});
