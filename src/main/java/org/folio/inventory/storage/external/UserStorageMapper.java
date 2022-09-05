package org.folio.inventory.storage.external;

import org.folio.inventory.domain.user.Personal;
import org.folio.inventory.domain.user.User;

import io.vertx.core.json.JsonObject;

class UserStorageMapper implements StorageMapper<User> {
  public User mapFromResponse(JsonObject userJson) {
    JsonObject personalJson = userJson.getJsonObject("personal");
    Personal personal = new Personal(personalJson.getString("lastName"),
      personalJson.getString("firstName"));

    return new User(userJson.getString("id"), personal);
  }

  public JsonObject mapToRequest(User user) {
    final var personal = user.getPersonal();

    final var personalJson = new JsonObject();

    includeIfPresent(personalJson, "lastName", personal.getLastName());
    includeIfPresent(personalJson, "firstName", personal.getFirstName());

    final var userJson = new JsonObject();

    includeIfPresent(userJson, "id", user.getId());
    includeIfPresent(userJson, "personal", personalJson);

    return userJson;
  }

  static <T>  void includeIfPresent(JsonObject instanceToSend,
    String propertyName, T propertyValue) {

    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }
}
