package org.folio.inventory.storage.external;

import org.folio.inventory.domain.Metadata;
import org.folio.inventory.domain.instances.Instance;

import io.vertx.core.json.JsonObject;

public class InstanceStorageMapper implements StorageMapper<Instance> {
  public Instance mapFromResponse(JsonObject representationFromServer) {
    return Instance.fromJson(representationFromServer)
      .setMetadata(new Metadata(
        representationFromServer.getJsonObject("metadata")));
  }

  public JsonObject mapToRequest(Instance instance) {
    return instance.getJsonForStorage();
  }
}
