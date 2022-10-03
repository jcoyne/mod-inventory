package org.folio.inventory.storage.external;

import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;

import io.vertx.core.http.HttpClient;

public class ExternalStorageCollections {
  private final String baseAddress;
  private final HttpClient client;

  public ExternalStorageCollections(String baseAddress, HttpClient client) {
    this.baseAddress = baseAddress;
    this.client = client;
  }

  public ItemCollection getItemCollection(String tenantId, String token) {
    return new ExternalStorageModuleItemCollection(baseAddress, tenantId, token,
      client);
  }

  public HoldingsRecordCollection getHoldingsRecordCollection(String tenantId, String token) {
    return new ExternalStorageModuleHoldingsRecordCollection(baseAddress,
      tenantId, token, client);
  }

  public InstanceCollection getInstanceCollection(String tenantId, String token) {
    return new ExternalStorageModuleInstanceCollection(baseAddress,
      tenantId, token, client);
  }

  public AuthorityRecordCollection getAuthorityCollection(String tenantId, String token) {
    return new ExternalStorageModuleAuthorityRecordCollection(baseAddress,
        tenantId, token, client);
  }

  public UserCollection getUserCollection(String tenantId, String token) {
    return new ExternalStorageModuleUserCollection(baseAddress,
      tenantId, token, client);
  }

  public HoldingsRecordsSourceCollection getHoldingsRecordsSourceCollection(String tenantId, String token) {
    return new ExternalStorageModuleHoldingsRecordsSourceCollection(baseAddress,
      tenantId, token, client);
  }
}
