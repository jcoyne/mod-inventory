package org.folio.inventory.storage.external;

import org.folio.inventory.domain.user.User;
import org.folio.inventory.domain.user.UserCollection;

import io.vertx.core.http.HttpClient;

class ExternalStorageModuleUserCollection
  extends ExternalStorageModuleCollection<User>
  implements UserCollection {

  ExternalStorageModuleUserCollection(
    String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "users"),
      "users", new StandardHeaders(tenant, token),
      client, User::getId, new UserStorageMapper());
  }
}
