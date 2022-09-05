package org.folio.inventory.storage.external;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;

import org.folio.HoldingsRecord;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

class HoldingsRecordStorageMapperTests {
  private final HoldingsRecordStorageMapper mapper = new HoldingsRecordStorageMapper();

  @Test
  void shouldMapToRequest() {
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();
    HoldingsRecord holdingsrecord = new HoldingsRecord()
      .withId(holdingId)
      .withInstanceId(instanceId)
      .withPermanentLocationId(permanentLocationId);

    JsonObject jsonObject = mapper.mapToRequest(holdingsrecord);

    assertNotNull(jsonObject);
    assertEquals(holdingId, jsonObject.getString("id"));
    assertEquals(instanceId, jsonObject.getString("instanceId"));
    assertEquals(permanentLocationId,
      jsonObject.getString("permanentLocationId"));
  }

  @Test
  void shouldMapFromJson() {
    String holdingId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String permanentLocationId = UUID.randomUUID().toString();
    JsonObject holdingsRecord = new JsonObject()
      .put("id", holdingId)
      .put("instanceId", instanceId)
      .put("permanentLocationId", permanentLocationId);

    HoldingsRecord holdingsrecord = mapper.mapFromResponse(holdingsRecord);

    assertNotNull(holdingsrecord);
    assertEquals(holdingId, holdingsrecord.getId());
    assertEquals(instanceId, holdingsrecord.getInstanceId());
    assertEquals(permanentLocationId,
      holdingsrecord.getPermanentLocationId());
  }

  @Test
  void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecord = new JsonObject()
      .put("testField", "testValue");

    assertThrows(JsonMappingException.class,
      () -> mapper.mapFromResponse(holdingsRecord));
  }
}
