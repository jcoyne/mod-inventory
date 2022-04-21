package org.folio.inventory.storage.external;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.BoundWithPart;

import io.vertx.core.json.JsonObject;

public class BoundWithPartsRepository {
  private final CollectionResourceClient client;

  public BoundWithPartsRepository(CollectionResourceClient client) {
    this.client = client;
  }

  public CompletableFuture<MultipleRecords<BoundWithPart>> fetchForItems(List<String> itemIds) {
    final var fetcher = MultipleRecordsFetchClient
      .builder()
      .withCollectionPropertyName("boundWithParts")
      .withExpectedStatus(200)
      .withCollectionResourceClient(client)
      .build();

    return fetcher
      .find(itemIds, this::cqlMatchAnyByItemIds)
      .thenApply(parts -> parts.stream().map(this::mapJsonToPart).collect(Collectors.toList()))
      .thenApply(parts -> new MultipleRecords<>(parts, parts.size()));
  }

  private BoundWithPart mapJsonToPart(JsonObject json) {
    return new BoundWithPart(json.getString("itemId"));
  }

  private CqlQuery cqlMatchAnyByItemIds(List<String> itemIds) {
    return CqlQuery.exactMatchAny( "itemId", itemIds );
  }
}
