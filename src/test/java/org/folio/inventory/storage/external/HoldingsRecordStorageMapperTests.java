package org.folio.inventory.storage.external;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.folio.HoldingsRecord;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class HoldingsRecordStorageMapperTests {
  private final HoldingsRecordStorageMapper mapper = new HoldingsRecordStorageMapper();

  @Test
  public void shouldMapToRequest() {
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
  public void shouldMapFromJson() {
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
    assertEquals(permanentLocationId, holdingsrecord.getPermanentLocationId());
  }

  @Test(expected = JsonMappingException.class)
  public void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecord = new JsonObject()
      .put("testField", "testValue");

    mapper.mapFromResponse(holdingsRecord);
  }
}
