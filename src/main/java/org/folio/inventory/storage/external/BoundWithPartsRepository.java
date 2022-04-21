package org.folio.inventory.storage.external;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.BoundWithPart;

import io.vertx.core.json.JsonObject;

public class BoundWithPartsRepository {
  private final MultipleRecordsFetchClient fetcher;

  public BoundWithPartsRepository(CollectionResourceClient client) {
    fetcher = MultipleRecordsFetchClient
      .builder()
      .withCollectionPropertyName("boundWithParts")
      .withExpectedStatus(200)
      .withCollectionResourceClient(client)
      .build();
  }

  public CompletableFuture<MultipleRecords<BoundWithPart>> findForItems(
    List<String> itemIds) {

    return fetcher
      .find(itemIds, this::cqlMatchAnyByItemIds)
      .thenApply(this::mapToParts)
      .thenApply(MultipleRecords::new);
  }

  public CompletableFuture<MultipleRecords<BoundWithPart>> findForHoldings(
    List<String> holdingsRecordIds) {

    return fetcher
      .find(holdingsRecordIds, this::cqlMatchAnyByHoldingsRecordIds)
      .thenApply(this::mapToParts)
      .thenApply(MultipleRecords::new);
  }

  private List<BoundWithPart> mapToParts(List<JsonObject> parts) {
    return parts.stream().map(this::mapJsonToPart).collect(Collectors.toList());
  }

  private BoundWithPart mapJsonToPart(JsonObject json) {
    return new BoundWithPart(json.getString("itemId"),
      json.getString("holdingsRecordId"));
  }

  private CqlQuery cqlMatchAnyByItemIds(Collection<String> itemIds) {
    return CqlQuery.exactMatchAny( "itemId", itemIds);
  }

  private CqlQuery cqlMatchAnyByHoldingsRecordIds(Collection<String> holdingsRecordIds) {
    return CqlQuery.exactMatchAny("holdingsRecordId", holdingsRecordIds);
  }
}
