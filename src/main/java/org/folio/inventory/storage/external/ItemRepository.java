package org.folio.inventory.storage.external;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.json.JsonObject;

public class ItemRepository {
  private final CollectionResourceClient client;

  public ItemRepository(CollectionResourceClient client) {
    this.client = client;
  }

  public CompletableFuture<List<JsonObject>> findForHoldings(
    List<String> holdingsRecordIds) {

    return MultipleRecordsFetchClient
      .builder()
      .withCollectionPropertyName("items")
      .withExpectedStatus(200)
      .withCollectionResourceClient(client)
      .build()
      .find(holdingsRecordIds, this::cqlMatchAnyByHoldingsRecordIds);
  }
  public CqlQuery cqlMatchAnyByHoldingsRecordIds(
    List<String> holdingsRecordIds) {
    return CqlQuery.exactMatchAny("holdingsRecordId", holdingsRecordIds);
  }
}
