package org.folio.inventory.storage.external;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Authority;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import io.vertx.core.json.JsonObject;

public class AuthorityStorageMapper implements StorageMapper<Authority> {
  private static final Logger LOGGER = LogManager.getLogger(AuthorityStorageMapper.class);

  public Authority mapFromResponse(JsonObject authorityFromServer) {
    try {
      return ObjectMapperTool.getMapper().readValue(authorityFromServer.encode(), Authority.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'Authority' entity", e);
    }
  }

  public JsonObject mapToRequest(Authority authority) {
    try {
      return JsonObject.mapFrom(authority);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map 'Authority' entity to json", e);
    }
  }
}
