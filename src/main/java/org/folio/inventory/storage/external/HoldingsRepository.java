package org.folio.inventory.storage.external;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.Holding;

import io.vertx.core.json.JsonObject;

public class HoldingsRepository {
  private final CollectionResourceClient client;

  public HoldingsRepository(CollectionResourceClient client) {
    this.client = client;
  }

  public CompletableFuture<MultipleRecords<Holding>> findForInstance(
    String instanceId) {

    final var fetcher = new MultipleRecordsFetchClient(client, "holdingsRecords");

    return fetcher.find(List.of(instanceId), this::byInstanceIds)
      .thenApply(this::mapToHoldings)
      .thenApply(MultipleRecords::new);
  }

  private List<Holding> mapToHoldings(List<JsonObject> holdings) {
    return holdings.stream().map(this::mapToHoldings).collect(Collectors.toList());
  }

  private Holding mapToHoldings(JsonObject json) {
    return new Holding(json.getString("id"),
      json.getString("instanceId"),
      json.getString("permanentLocationId"));
  }

  private CqlQuery byInstanceIds(List<String> ids) {
    return CqlQuery.exactMatchAny("instanceId", ids);
  }
}
