package org.folio.inventory.storage.external;

import org.folio.HoldingsRecordsSource;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;

import io.vertx.core.http.HttpClient;

public class ExternalStorageModuleHoldingsRecordsSourceCollection
  extends ExternalStorageModuleCollection<HoldingsRecordsSource>
  implements HoldingsRecordsSourceCollection {

  ExternalStorageModuleHoldingsRecordsSourceCollection(
    String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-sources"),
      tenant, token, "holdingsRecordsSources", client,
      HoldingsRecordsSource::getId, new HoldingsRecordsSourceStorageMapper());
  }
}
