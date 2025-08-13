const axios = require('axios');
const cds = require('@sap/cds');
const DEBUG = cds.debug('attachments');
const smClient = require('./SMClient.js')
const https = require("https");

async function bindObjectStoreInstance(sm_url, tenant, instanceID, token) {
    if (instanceID) {
        try {
            const response = await axios.post(`${sm_url}/${PATH.SERVICE_BINDING}`, {
                name: `object-store-${tenant}-${cds.utils.uuid()}`,
                service_instance_id: instanceID,
                parameters: {},
                labels: { tenant_id: [tenant], service: ["OBJECT_STORE"] }
            }, {
                headers: {
                    'Accept': 'application/json',
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                }
            });
            return response.data.id;
        } catch (error) {
            console.log(`Error binding object store instance for tenant - ${tenant}: ${error.message}`);
        }
    }
};

async function createObjectStoreInstance(sm_url, tenant, planID, token) {
    try {
        const response = await axios.post(`${sm_url}/v1/service_instances`, {
            name: `object-store-${tenant}-${cds.utils.uuid()}`,
            service_plan_id: planID,
            parameters: {},
            labels: { tenant_id: [tenant], service: ["OBJECT_STORE"] }
        }, {
            headers: {
                'Accept': 'application/json',
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        });
        const instancePath = response.headers.location.substring(1);
        const instanceId = await _pollUntilDone(sm_url, instancePath, token);
        return instanceId.data.resource_id;
    } catch (error) {
        console.log(`Error creating object store instance - ${tenant}: ${error.message}`);
    }
};

async function getBindingIdForDeletion(sm_url, tenant, token){
    try {
        const getBindingCredentials = await smClient.serviceManagerRequest(sm_url, HTTP_METHOD.GET, PATH.SERVICE_BINDING, token, {
            labelQuery: `service eq 'OBJECT_STORE' and tenant_id eq '${tenant}'`
        });
        if (!getBindingCredentials?.id) {
            console.log("No binding credentials found!");
            return null; // Handle missing data gracefully
        }
        return getBindingCredentials.id;

    } catch (error) {
        console.log(`Error fetching binding credentials for tenant - ${tenant}: ${error.message}`);
    }
};

async function getInstanceIdForDeletion(sm_url, tenant, token){
    try {
        const instanceId = await smClient.serviceManagerRequest(sm_url, HTTP_METHOD.GET, PATH.SERVICE_INSTANCE, token, { labelQuery: `service eq 'OBJECT_STORE' and tenant_id eq '${tenant}'` });
        return instanceId.id;
    } catch (error) {
        console.log(`Error fetching service instance id for tenant - ${tenant}: ${error.message}`);
    }
}

async function deleteObjectStoreInstance(sm_url, instanceID, token){
    if (instanceID) {
        try {
            const response = await axios.delete(`${sm_url}/${PATH.SERVICE_INSTANCE}/${instanceID}`, {
                headers: { 'Accept': 'application/json', 'Authorization': `Bearer ${token}` }
            });
            const instancePath = response.headers.get("location").substring(1);
            await _pollUntilDone(sm_url, instancePath, token); // remove
            console.log('Object Store instance deleted');
        } catch (error) {
            console.log(`Error deleting object store instance - ${instanceID}: ${error.message}`);
        }
    }
};


module.exports = {
  createObjectStoreInstance,
  bindObjectStoreInstance,
  getBindingIdForDeletion,
  getInstanceIdForDeletion,
  deleteObjectStoreInstance
};