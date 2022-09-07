package org.folio.inventory.storage.external;

import java.util.UUID;

import org.folio.inventory.domain.Holding;

import io.vertx.core.json.JsonObject;

public class HoldingStorageMapper implements StorageMapper<Holding> {
  public JsonObject mapToRequest(Holding holding) {
    JsonObject holdingToSend = new JsonObject();

    holdingToSend.put("id", holding.id != null
      ? holding.id
      : UUID.randomUUID().toString());

    ExternalStorageModuleCollection.includeIfPresent(holdingToSend,
      "instanceId", holding.instanceId);
    ExternalStorageModuleCollection.includeIfPresent(holdingToSend,
      "permanentLocationId", holding.permanentLocationId);

    return holdingToSend;
  }

  public Holding mapFromResponse(JsonObject representationFromServer) {
    return new Holding(
      representationFromServer.getString("id"),
      representationFromServer.getString("instanceId"),
      representationFromServer.getString("permanentLocationId"));
  }
}
