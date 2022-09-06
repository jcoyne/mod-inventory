package org.folio.inventory.storage.external;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.folio.HoldingsRecordsSource;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

class HoldingsRecordSourceMapperTests {
  private final HoldingsRecordsSourceStorageMapper mapper = new HoldingsRecordsSourceStorageMapper();

  @Test
  void shouldMapFromJson() {
    String sourceId = UUID.randomUUID().toString();
    String name = "MARC";
    JsonObject holdingsRecordsSource = new JsonObject()
      .put("id", sourceId)
      .put("name", name);

    HoldingsRecordsSource source = mapper.mapFromResponse(holdingsRecordsSource);
    assertNotNull(source);
    assertEquals(sourceId, source.getId());
    assertEquals(name, source.getName());
  }

  @Test()
  void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecordsSource = new JsonObject()
      .put("testField", "testValue");

    assertThrows(JsonMappingException.class,
      () -> mapper.mapFromResponse(holdingsRecordsSource));
  }

  @Test
  void shouldMapToRequest() {
    String sourceId = UUID.randomUUID().toString();
    String name = "MARC";
    HoldingsRecordsSource holdingsRecordsSource = new HoldingsRecordsSource()
      .withId(sourceId)
      .withName(name);

    JsonObject jsonObject = mapper.mapToRequest(holdingsRecordsSource);
    assertNotNull(jsonObject);
    assertEquals(sourceId, jsonObject.getString("id"));
    assertEquals(name, jsonObject.getString("name"));
  }
}
