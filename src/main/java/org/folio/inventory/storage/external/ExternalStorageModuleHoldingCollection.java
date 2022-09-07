package org.folio.inventory.storage.external;

import org.folio.inventory.domain.Holding;
import org.folio.inventory.domain.HoldingCollection;

import io.vertx.core.http.HttpClient;

class ExternalStorageModuleHoldingCollection
  extends ExternalStorageModuleCollection<Holding>
  implements HoldingCollection {

  ExternalStorageModuleHoldingCollection(String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-storage/holdings"),
      "holdingsRecords", new StandardHeaders(tenant, token),
      client, Holding::getId, new HoldingStorageMapper());
  }
}
