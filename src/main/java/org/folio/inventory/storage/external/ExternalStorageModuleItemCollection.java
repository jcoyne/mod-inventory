package org.folio.inventory.storage.external;

import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;

import io.vertx.core.http.HttpClient;

class ExternalStorageModuleItemCollection
  extends ExternalStorageModuleCollection<Item>
  implements ItemCollection {

  ExternalStorageModuleItemCollection(String baseAddress, String tenant,
    String token, HttpClient client) {

    super(String.format("%s/%s", baseAddress, "item-storage/items"),
      "items", new StandardHeaders(tenant, token),
      client, Item::getId, new ItemStorageMapper());
  }
}
