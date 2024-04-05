const { getSDMCredentials, getConfigurations } = require("../util/index");
const axios = require("axios").default;
const FormData = require("form-data");

function createAttachment(data) {
  const token = getSDMCredentials();
  const objectId = createDocument(data, token);
  console.log("Creating attachment");
}

function deleteAttachment() {
  getSDMCredentials();
}

function createDocument(data, token) {
  const { repositoryId } = getConfigutations();
  const creds = this.options.credentials;
  this.sdmurl = creds.uri;
  const documentCreateURL = this.sdmurl + "browser/" + repositoryId + "/root";
  const formData = new FormData();
  formData.append("cmisaction", "createDocument");
  //   formData.append("propertyId[0]", "cmis:name");
  //   formData.append("propertyValue[0]", result[0].filename);
  //   formData.append("propertyId[1]", "cmis:objectTypeId");
  //   formData.append("propertyValue[1]", "cmis:document");
  formData.append("succinct", "true");
  formData.append("filename", data);
  //   formData.append("filename", req.data.content, {
  //     name: "file",
  //     filename: result[0].name,
  //   });

  let headers = formData.getHeaders();
  headers["Authorization"] = "Bearer " + token;
  const config = {
    headers: headers,
  };
  axios
    .post(documentCreateURL, formData, config)
    .then((response) => {
      console.log("Res " + response.data.succinctProperties["cmis:objectId"]);
      return response.data.succinctProperties["cmis:objectId"];
    })
    .catch((error) => {
      console.error("Error " + error);
    });
}

module.exports = {
  createAttachment,
  deleteAttachment,
};
