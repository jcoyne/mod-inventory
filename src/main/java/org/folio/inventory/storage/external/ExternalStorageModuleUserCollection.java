package org.folio.inventory.storage.external;

import org.folio.inventory.domain.user.Personal;
import org.folio.inventory.domain.user.User;
import org.folio.inventory.domain.user.UserCollection;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleUserCollection
  extends ExternalStorageModuleCollection<User>
  implements UserCollection {

  private static JsonObject mapToRequest(User user) {
    final var personal = user.getPersonal();

    final var personalJson = new JsonObject();

    includeIfPresent(personalJson, "lastName", personal.getLastName());
    includeIfPresent(personalJson,"firstName", personal.getFirstName());

    final var userJson = new JsonObject();

    includeIfPresent(userJson,"id", user.getId());
    includeIfPresent(userJson,"personal", personalJson);

    return userJson;
  }

  public static User mapFromResponse(JsonObject userJson) {
    JsonObject personalJson = userJson.getJsonObject("personal");
    Personal personal = new Personal(personalJson.getString("lastName"),
      personalJson.getString("firstName"));

    return new User(userJson.getString("id"), personal);
  }

  ExternalStorageModuleUserCollection(
    String baseAddress,
    String tenant,
    String token,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "users"),
      tenant, token, "users", client,
      User::getId, ExternalStorageModuleUserCollection::mapToRequest,
      ExternalStorageModuleUserCollection::mapFromResponse);
  }

}
