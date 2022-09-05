package org.folio.inventory.storage.external;

import io.vertx.core.json.JsonObject;

public interface StorageMapper<T> {
  JsonObject mapToRequest(T entity);
  T mapFromResponse(JsonObject representationFromResponse);
}
