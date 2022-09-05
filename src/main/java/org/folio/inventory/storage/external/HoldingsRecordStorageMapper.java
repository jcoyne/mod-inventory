package org.folio.inventory.storage.external;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import io.vertx.core.json.JsonObject;

class HoldingsRecordStorageMapper implements StorageMapper<HoldingsRecord> {
  private static final Logger LOGGER = LogManager.getLogger(HoldingsRecordStorageMapper.class);

  @Override
  public JsonObject mapToRequest(HoldingsRecord entity) {
    try {
      return JsonObject.mapFrom(entity);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map 'Holdingsrecord' entity to json", e);
    }
  }

  @Override
  public HoldingsRecord mapFromResponse(JsonObject representationFromResponse) {
    try {
      return ObjectMapperTool.getMapper()
        .readValue(representationFromResponse.encode(), HoldingsRecord.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'Holdingsrecord' entity", e);
    }
  }
}
