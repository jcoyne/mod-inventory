package org.folio.inventory.storage.external;

import java.util.UUID;

import org.folio.inventory.domain.Holding;

import io.vertx.core.json.JsonObject;

public class HoldingStorageMapper implements StorageMapper<Holding> {
  public static <T>  void includeIfPresent(JsonObject instanceToSend,
    String propertyName, T propertyValue) {

    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }

  public JsonObject mapToRequest(Holding holding) {
    JsonObject holdingToSend = new JsonObject();

    holdingToSend.put("id", holding.id != null
      ? holding.id
      : UUID.randomUUID().toString());

    includeIfPresent(holdingToSend,
      "instanceId", holding.instanceId);
    includeIfPresent(holdingToSend,
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
