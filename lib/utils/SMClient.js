const axios = require('axios');
const cds = require('@sap/cds');
const DEBUG = cds.debug('attachments');
const https = require("https");



function validateSMCredentials({ sm_url, url, clientid, clientsecret, certificate, key }) {
  if (!sm_url || !url) {
    throw new Error("Missing Service Manager credentials: 'sm_url' or 'url' is not defined.");
  }

  if (!clientid || !clientsecret) {
    DEBUG?.("Client credentials not found. Falling back to MTLS if certificate and key are provided.");
    if (!certificate || !key) {
      throw new Error("MTLS credentials are also missing: 'certificate' or 'key' is not defined.");
    }
  }
};

async function serviceManagerRequest(sm_url, method, path, token, params = {}) {
    try {
        const response = await axios({
            method,
            url: `${sm_url}/${path}`,
            headers: {
                'Accept': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            params
        });

        return response?.data?.items?.[0]; // Error handling : return undefined instead of crashing when .items is undefined

    } catch (error) {
        DEBUG?.(`Error fetching data from service manager: ${error.message}`);
    }
};

async function getPlanID(sm_url, token, offeringID) {
  // Recheck the fieldQuery for catalog_name
  const supportedPlans = ["standard", "s3-standard"];
  for (const planName of supportedPlans) {
    try {
      const plan = await serviceManagerRequest(
        sm_url,
        HTTP_METHOD.GET,
        PATH.SERVICE_PLAN,
        token,
        {
          fieldQuery: `service_offering_id eq '${offeringID}' and catalog_name eq '${planName}'`,
        }
      );
      if (plan?.id) {
        DEBUG?.(`Using object store plan "${planName}" with ID: ${plan.id}`);
        return plan.id;
      }
    } catch (error) {
      DEBUG?.(`Failed to fetch plan "${planName}": ${error.message}`);
    }
  }
  DEBUG?.(
    `No valid object store plan found (attempted: ${supportedPlans.join(", ")})`
  );
  throw new Error(
    `No supported object store service plan found in Service Manager.`
  );
};

async function getOfferingID(sm_url, token) {
  const offerings = await _serviceManagerRequest(sm_url, HTTP_METHOD.GET, PATH.SERVICE_OFFERING, token, { fieldQuery: "name eq 'objectstore'" });
  const offeringID = offerings.id;
  if (!offeringID) DEBUG?.('Object store service offering not found');
  return offeringID;
}



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
            DEBUG?.(`Error binding object store instance for tenant - ${tenant}: ${error.message}`);
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
        DEBUG?.(`Error creating object store instance - ${tenant}: ${error.message}`);
    }
};

async function getServiceBindingCredentials(tenantID, plan, sm_url, token) {
  try {
    const response = await axios.get(`${sm_url}/v1/service_bindings`, {
      params: { labelQuery: `service eq '${plan}' and tenant_id eq '${tenantID}'` },
      headers: {
        'Accept': 'application/json',
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      }
    });

    return response.data.items[0]; 
  } catch (error) {
    DEBUG?.(`Error fetching object store credentials: ${error.message}`);
  }
}


async function getBindingIdForDeletion(sm_url, tenant, token){
    try {
        const getBindingCredentials = await smClient.serviceManagerRequest(sm_url, HTTP_METHOD.GET, PATH.SERVICE_BINDING, token, {
            labelQuery: `service eq 'OBJECT_STORE' and tenant_id eq '${tenant}'`
        });
        if (!getBindingCredentials?.id) {
            DEBUG?.("No binding credentials found!");
            return null; // Handle missing data gracefully
        }
        return getBindingCredentials.id;

    } catch (error) {
        DEBUG?.(`Error fetching binding credentials for tenant - ${tenant}: ${error.message}`);
    }
};

async function deleteBinding(sm_url, bindingID, token){
    if (bindingID) {
        try {
            await axios.delete(`${sm_url}/${PATH.SERVICE_BINDING}/${bindingID}`, {
                headers: { 'Accept': 'application/json', 'Authorization': `Bearer ${token}` }
            });
        } catch (error) {
            DEBUG?.('Error deleting binding:', error.message);
        }
    } else {
        DEBUG?.("Binding id is either undefined or null");
    }
};

async function getInstanceIdForDeletion(sm_url, tenant, token){
    try {
        const instanceId = await smClient.serviceManagerRequest(sm_url, HTTP_METHOD.GET, PATH.SERVICE_INSTANCE, token, { labelQuery: `service eq 'OBJECT_STORE' and tenant_id eq '${tenant}'` });
        return instanceId.id;
    } catch (error) {
        DEBUG?.(`Error fetching service instance id for tenant - ${tenant}: ${error.message}`);
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
            DEBUG?.('Object Store instance deleted');
        } catch (error) {
            DEBUG?.(`Error deleting object store instance - ${instanceID}: ${error.message}`);
        }
    }
};


async function fetchToken(url, clientid, clientsecret, certificate, key, certURL) {
  if (certificate && key && certURL) {
    return fetchTokenWithMTLS(certURL, clientid, certificate, key);
  } else if (clientid && clientsecret) {
    return fetchTokenWithClientSecret(url, clientid, clientsecret);
  } else {
    throw new Error("Invalid credentials provided for token fetching.");
  }
}

async function fetchTokenWithClientSecret(url, clientid, clientsecret) {
  try {
    DEBUG?.("Using OAuth client credentials to fetch token.");
    const headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/x-www-form-urlencoded'
    };
    const response = await axios.post(`${url}/oauth/token`, null, {
      headers,
      params: {
        grant_type: "client_credentials",
        client_id: clientid,
        client_secret: clientsecret,
      },
    });
    return response.data.access_token;
  } catch (error) {
    DEBUG?.(`Error fetching token for client credentials: ${error.message}`);
    throw error;
  }
}

async function fetchTokenWithMTLS(certURL, clientid, certificate, key) {
  try {
    DEBUG?.("Using MTLS certificate/key to fetch token.");

    const requestBody = new URLSearchParams({
      grant_type: 'client_credentials',
      response_type: 'token',
      client_id: clientid
    }).toString()

    const options = {
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'    
      },
      url: `${certURL}/oauth/token`,  
      method: 'POST',
      data: requestBody,
      httpsAgent: new https.Agent({
        cert: certificate,
        key: key
      })
    }
    const response = await axios(options);
    return response.data.access_token;
  } catch (error) {
    DEBUG?.(`Error fetching token with MTLS: ${error.message}`);
    throw error;
  }
}

module.exports = {
  fetchToken,
  getServiceBindingCredentials,
  getOfferingID,
  getPlanID,
  createObjectStoreInstance,
  validateSMCredentials,
  bindObjectStoreInstance,
  getBindingIdForDeletion,
  deleteBinding,
  getInstanceIdForDeletion,
  deleteObjectStoreInstance
};