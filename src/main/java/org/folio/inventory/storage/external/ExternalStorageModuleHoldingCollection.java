package org.folio.inventory.storage.external;

import java.util.UUID;

import org.folio.inventory.domain.Holding;
import org.folio.inventory.domain.HoldingCollection;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleHoldingCollection
  extends ExternalStorageModuleCollection<Holding>
  implements HoldingCollection {

  private static JsonObject mapToRequest(Holding holding) {
    JsonObject holdingToSend = new JsonObject();

    holdingToSend.put("id", holding.id != null
      ? holding.id
      : UUID.randomUUID().toString());

    includeIfPresent(holdingToSend, "instanceId", holding.instanceId);
    includeIfPresent(holdingToSend, "permanentLocationId", holding.permanentLocationId);

    return holdingToSend;
  }

  public static Holding mapFromJsonStatic(JsonObject holdingFromServer) {
    return new Holding(
      holdingFromServer.getString("id"),
      holdingFromServer.getString("instanceId"),
      holdingFromServer.getString("permanentLocationId"));
  }

  ExternalStorageModuleHoldingCollection(String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-storage/holdings"),
      tenant, token, "holdingsRecords", client,
      Holding::getId, ExternalStorageModuleHoldingCollection::mapToRequest,
      ExternalStorageModuleHoldingCollection::mapFromJsonStatic);
  }

  @Override
  protected Holding mapFromJson(JsonObject holdingFromServer) {
    return new Holding(
      holdingFromServer.getString("id"),
      holdingFromServer.getString("instanceId"),
      holdingFromServer.getString("permanentLocationId"));
  }
}
