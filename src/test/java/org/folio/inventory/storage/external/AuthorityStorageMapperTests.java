package org.folio.inventory.storage.external;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;

import org.folio.Authority;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

class AuthorityStorageMapperTests {
  private static final String AUTHORITY_ID = UUID.randomUUID().toString();
  private static final String CORPORATE_NAME = UUID.randomUUID().toString();
  private static final Integer VERSION = 3;

  private final AuthorityStorageMapper mapper = new AuthorityStorageMapper();

  @Test
  void shouldMapFromJson() {
    JsonObject authorityRecord = new JsonObject()
      .put("id", AUTHORITY_ID)
      .put("_version", VERSION)
      .put("corporateName", CORPORATE_NAME);

    Authority authority = mapper.mapFromResponse(authorityRecord);
    assertNotNull(authority);
    assertEquals(AUTHORITY_ID, authority.getId());
    assertEquals(VERSION, authority.getVersion());
    assertEquals(CORPORATE_NAME, authority.getCorporateName());
  }

  @Test
  void shouldNotMapFromJsonAndThrowException() {
    JsonObject holdingsRecord = new JsonObject()
      .put("_version", "wrongFormat");

    assertThrows(JsonMappingException.class,
      () -> mapper.mapFromResponse(holdingsRecord));
  }

  @Test
  void shouldMapToRequest() {
    Authority authority = new Authority()
      .withId(AUTHORITY_ID)
      .withVersion(VERSION)
      .withCorporateName(CORPORATE_NAME);

    JsonObject jsonObject = mapper.mapToRequest(authority);
    assertNotNull(jsonObject);
    assertEquals(AUTHORITY_ID, jsonObject.getString("id"));
    assertEquals(VERSION.toString(), jsonObject.getString("_version"));
    assertEquals(CORPORATE_NAME, jsonObject.getString("corporateName"));
  }
}
