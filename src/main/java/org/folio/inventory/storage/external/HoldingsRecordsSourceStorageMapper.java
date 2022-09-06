package org.folio.inventory.storage.external;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecordsSource;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import io.vertx.core.json.JsonObject;

public class HoldingsRecordsSourceStorageMapper implements StorageMapper<HoldingsRecordsSource> {
  private static final Logger LOGGER = LogManager.getLogger(HoldingsRecordsSourceStorageMapper.class);

  public HoldingsRecordsSource mapFromResponse(JsonObject representationFromServer) {
    try {
      return ObjectMapperTool.getMapper()
        .readValue(representationFromServer.encode(), HoldingsRecordsSource.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'holdingsRecordsSources' entity", e);
    }
  }

  public JsonObject mapToRequest(HoldingsRecordsSource entity) {
    return JsonObject.mapFrom(entity);
  }
}
