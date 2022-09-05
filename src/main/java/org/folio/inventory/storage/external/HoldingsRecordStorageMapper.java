package org.folio.inventory.storage.external;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import io.vertx.core.json.JsonObject;

class HoldingsRecordStorageMapper {
  private static final Logger LOGGER = LogManager.getLogger(HoldingsRecordStorageMapper.class);

  JsonObject mapToRequest(HoldingsRecord holding) {
    try {
      return JsonObject.mapFrom(holding);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map 'Holdingsrecord' entity to json", e);
    }
  }

  HoldingsRecord mapFromResponse(JsonObject holdingFromServer) {
    try {
      return ObjectMapperTool.getMapper()
        .readValue(holdingFromServer.encode(), HoldingsRecord.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'Holdingsrecord' entity", e);
    }
  }
}
