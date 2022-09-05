package org.folio.inventory.storage.external;

import org.folio.HoldingsRecord;
import org.folio.inventory.domain.HoldingsRecordCollection;

import io.vertx.core.http.HttpClient;

class ExternalStorageModuleHoldingsRecordCollection
  extends ExternalStorageModuleCollection<HoldingsRecord>
  implements HoldingsRecordCollection {

  ExternalStorageModuleHoldingsRecordCollection(String baseAddress,
                                         String tenant,
                                         String token,
                                         HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-storage/holdings"),
      tenant, token, "holdingsRecords", client,
      HoldingsRecord::getId,
      new HoldingsRecordStorageMapper()::mapToRequest,
      new HoldingsRecordStorageMapper()::mapFromResponse);
  }
}
