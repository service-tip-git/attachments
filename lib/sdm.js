const cds = require("@sap/cds");
const { createAttachment } = require("./lib/sdm/handler/index");
const { SELECT } = cds.ql;

module.exports = class SDMAttachmentsService extends require("./basic") {
  async get(attachments, keys) {
    const response = await SELECT.from(attachments, keys).columns("url");
    if (response?.url) {
      const Key = response.url;

      const content = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key,
        })
      );
      return content.Body;
    }
  }

  async draftSaveHandler() {
    let attachmentsToDelete = await SELECT.from(attachments)
      .columns("url")
      .where({
        ID: { "not in": [await SELECT.from(attachments.drafts).columns("id")] },
      });

    let attachmentsToCreate = await SELECT.from(attachments.drafts)
      .columns("url")
      .where({
        ID: { "not in": [await SELECT.from(attachments).columns("id")] },
      });

    if (attachmentsToDelete.length != 0) {
      this.onDelete();
    }

    if (attachmentsToCreate.length != 0) {
      this.onCreate(attachmentsToCreate);
    }
  }
  onCreate(data) {
    for (let i = 0; i < data.length; i++) {
      objectID = createAttachment(data[i]);
      //save to attachment table
    }
  }
  onDelete(data) {}

  registerUpdateHandlers(srv, entity, mediaElement) {
    srv.on("SAVE", entity, this.draftSaveHandler);
    return;
  }

  async delete(Key) {
    const response = await this.client.send(
      new DeleteObjectCommand({
        Bucket: this.bucket,
        Key,
      })
    );
    return response.DeleteMarker;
  }
};
