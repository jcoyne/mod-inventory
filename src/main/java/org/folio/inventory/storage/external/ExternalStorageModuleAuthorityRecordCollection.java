package org.folio.inventory.storage.external;

import org.folio.Authority;
import org.folio.inventory.domain.AuthorityRecordCollection;

import io.vertx.core.http.HttpClient;

public class ExternalStorageModuleAuthorityRecordCollection
  extends ExternalStorageModuleCollection<Authority>
  implements AuthorityRecordCollection {

  ExternalStorageModuleAuthorityRecordCollection(String baseAddress,
    String tenant, String token, HttpClient client) {

    super(String.format("%s/%s", baseAddress, "authority-storage/authorities"),
      tenant, token, "authorities", client,
      Authority::getId, new AuthorityStorageMapper());
  }
}
