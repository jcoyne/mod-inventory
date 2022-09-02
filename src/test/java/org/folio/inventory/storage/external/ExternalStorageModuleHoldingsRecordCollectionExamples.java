package org.folio.inventory.storage.external;

import static org.folio.inventory.storage.external.ExternalStorageModuleHoldingsRecordCollection.mapFromResponse;
import static org.folio.inventory.storage.external.ExternalStorageModuleHoldingsRecordCollection.mapToRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.folio.HoldingsRecord;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class ExternalStorageModuleHoldingsRecordCollectionExamples extends ExternalStorageTests {
  private final ExternalStorageModuleHoldingsRecordCollection storage =
    useHttpClient(client -> new ExternalStorageModuleHoldingsRecordCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, client));

  @Test
  public void shouldMapFromJson() {
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();
    JsonObject holdingsRecord = new JsonObject()
      .put("id", holdingId)
      .put("instanceId", instanceId)
      .put("permanentLocationId", permanentLocationId);

    HoldingsRecord holdingsrecord = mapFromResponse(holdingsRecord);
    assertNotNull(holdingsrecord);
    assertEquals(holdingId, holdingsrecord.getId());
    assertEquals(instanceId, holdingsrecord.getInstanceId());
    assertEquals(permanentLocationId, holdingsrecord.getPermanentLocationId());
  }

  @Test(expected = JsonMappingException.class)
  public void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecord = new JsonObject()
      .put("testField", "testValue");

    mapFromResponse(holdingsRecord);
  }

  @Test
  public void shouldMapToRequest() {
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();
    HoldingsRecord holdingsrecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withPermanentLocationId(permanentLocationId);

    JsonObject jsonObject = mapToRequest(holdingsrecord);
    assertNotNull(jsonObject);
    assertEquals(holdingId, jsonObject.getString("id"));
    assertEquals(instanceId, jsonObject.getString("instanceId"));
    assertEquals(permanentLocationId, jsonObject.getString("permanentLocationId"));
  }
}
