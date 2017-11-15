package org.folio.inventory.storage.external;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.Creator;
import org.folio.inventory.domain.Identifier;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;
import org.folio.inventory.support.JsonArrayHelper;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.folio.inventory.support.JsonArrayHelper.toList;

class ExternalStorageModuleInstanceCollection
  implements InstanceCollection {

  private final Vertx vertx;
  private final String storageAddress;
  private final String tenant;
  private final String token;

  ExternalStorageModuleInstanceCollection(
    Vertx vertx,
    String storageAddress,
    String tenant,
    String token) {

    this.vertx = vertx;
    this.storageAddress = storageAddress;
    this.tenant = tenant;
    this.token = token;
  }

  @Override
  public void add(Instance item,
    Consumer<Success<Instance>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = storageAddress + "/instance-storage/instances";

    Handler<HttpClientResponse> onResponse = response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 201) {
          Instance createdInstance = mapFromJson(new JsonObject(responseBody));

          resultCallback.accept(new Success<>(createdInstance));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
    });

    JsonObject itemToSend = mapToInstanceRequest(item);

    HttpClientRequest request = createRequest(HttpMethod.POST, location,
      onResponse, failureCallback);

    jsonContentType(request);
    acceptJson(request);

    request.end(Json.encodePrettily(itemToSend));
  }

  @Override
  public void findById(String id,
    Consumer<Success<Instance>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = individualRecordLocation(id);

    Handler<HttpClientResponse> onResponse = response -> response.bodyHandler(buffer -> {
      String responseBody = buffer.getString(0, buffer.length());
      int statusCode = response.statusCode();

      switch (statusCode) {
        case 200:
          JsonObject instanceFromServer = new JsonObject(responseBody);

          Instance foundInstance = mapFromJson(instanceFromServer);

          resultCallback.accept(new Success<>(foundInstance));
          break;

        case 404:
          resultCallback.accept(new Success<>(null));
          break;

        default:
          failureCallback.accept(new Failure(responseBody, statusCode));
      }
    });

    HttpClientRequest request = createRequest(HttpMethod.GET, location,
      onResponse, failureCallback);

    acceptJson(request);
    request.end();
  }

  @Override
  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<Instance>>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = String.format(storageAddress
        + "/instance-storage/instances?limit=%s&offset=%s",
      pagingParameters.limit, pagingParameters.offset);

    HttpClientRequest request = createRequest(HttpMethod.GET, location,
      handleMultipleResults(resultCallback, failureCallback), failureCallback);

    acceptJson(request);
    request.end();
  }

  @Override
  public void empty(
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {
    String location = storageAddress + "/instance-storage/instances";

    Handler<HttpClientResponse> onResponse = noContentResponseHandler(
      completionCallback, failureCallback);

    HttpClientRequest request =
      createRequest(HttpMethod.DELETE, location, onResponse, failureCallback);

    acceptJsonOrPlainText(request);
    request.end();

  }

  @Override
  public void findByCql(String cqlQuery,
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<Instance>>> resultCallback,
    Consumer<Failure> failureCallback) throws UnsupportedEncodingException {

    String encodedQuery = URLEncoder.encode(cqlQuery, "UTF-8");

    String location =
      String.format("%s/instance-storage/instances?query=%s", storageAddress, encodedQuery) +
        String.format("&limit=%s&offset=%s", pagingParameters.limit,
          pagingParameters.offset);

    HttpClientRequest request = createRequest(HttpMethod.GET, location,
      handleMultipleResults(resultCallback, failureCallback), failureCallback);

    acceptJson(request);
    request.end();
  }

  @Override
  public void update(Instance item,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    String location = individualRecordLocation(item.id);

    Handler<HttpClientResponse> onResponse = noContentResponseHandler(
      completionCallback, failureCallback);

    JsonObject itemToSend = mapToInstanceRequest(item);

    HttpClientRequest request = createRequest(HttpMethod.PUT, location,
      onResponse, failureCallback);

    jsonContentType(request);
    acceptPlainText(request);

    request.end(Json.encodePrettily(itemToSend));
  }

  @Override
  public void delete(String id,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {
    String location = individualRecordLocation(id);

    Handler<HttpClientResponse> onResponse = noContentResponseHandler(
      completionCallback, failureCallback);

    HttpClientRequest request = createRequest(HttpMethod.DELETE, location,
      onResponse, failureCallback);

    acceptJsonOrPlainText(request);
    request.end();
  }

  private JsonObject mapToInstanceRequest(Instance instance) {
    JsonObject instanceToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    instanceToSend.put("id", instance.id != null ? instance.id : UUID.randomUUID().toString());
    instanceToSend.put("title", instance.title);
    includeIfPresent(instanceToSend, "instanceTypeId", instance.instanceTypeId);
    includeIfPresent(instanceToSend, "source", instance.source);
    instanceToSend.put("identifiers", instance.identifiers);
    instanceToSend.put("creators", instance.creators);

    return instanceToSend;
  }

  private Instance mapFromJson(JsonObject instanceFromServer) {

    List<JsonObject> identifiers = toList(
      instanceFromServer.getJsonArray("identifiers", new JsonArray()));

    List<Identifier> mappedIdentifiers = identifiers.stream()
      .map(it -> new Identifier(it.getString("identifierTypeId"), it.getString("value")))
      .collect(Collectors.toList());

    List<JsonObject> creators = toList(
      instanceFromServer.getJsonArray("creators", new JsonArray()));

    List<Creator> mappedCreators = creators.stream()
      .map(it -> new Creator(it.getString("creatorTypeId"), it.getString("name")))
      .collect(Collectors.toList());

    return new Instance(
      instanceFromServer.getString("id"),
      instanceFromServer.getString("title"),
      mappedIdentifiers,
      instanceFromServer.getString("source"),
      instanceFromServer.getString("instanceTypeId"),
      mappedCreators);
  }

  private void includeIfPresent(
    JsonObject instanceToSend,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      instanceToSend.put(propertyName, propertyValue);
    }
  }

  private Handler<Throwable> exceptionHandler(Consumer<Failure> failureCallback) {
    return it -> failureCallback.accept(new Failure(it.getMessage(), null));
  }

  private Handler<HttpClientResponse> handleMultipleResults(
    Consumer<Success<MultipleRecords<Instance>>> resultCallback,
    Consumer<Failure> failureCallback) {

    return response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 200) {
          JsonObject wrappedInstances = new JsonObject(responseBody);

          List<JsonObject> instances = JsonArrayHelper.toList(
            wrappedInstances.getJsonArray("instances"));

          List<Instance> foundItems = instances.stream()
            .map(this::mapFromJson)
            .collect(Collectors.toList());

          MultipleRecords<Instance> result = new MultipleRecords<>(
            foundItems, wrappedInstances.getInteger("totalRecords"));

          resultCallback.accept(new Success<>(result));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
      });
  }

  private void acceptJson(HttpClientRequest request) {
    request.putHeader("Accept", "application/json");
  }

  private void jsonContentType(HttpClientRequest request) {
    accept(request, "application/json");
  }

  private static void acceptJsonOrPlainText(HttpClientRequest request) {
    accept(request, "application/json, text/plain");
  }

  private static void acceptPlainText(HttpClientRequest request) {
    accept(request, "text/plain");
  }

  private static void accept(
    HttpClientRequest request,
    String contentTypes) {

    request.putHeader("Accept", contentTypes);
  }

  private void addOkapiHeaders(HttpClientRequest request) {
    request.putHeader("X-Okapi-Tenant", tenant)
      .putHeader("X-Okapi-Token", token);
  }

  private void registerExceptionHandler(
    HttpClientRequest request,
    Consumer<Failure> failureCallback) {

    request.exceptionHandler(exceptionHandler(failureCallback));
  }

  private HttpClientRequest createRequest(
    HttpMethod method,
    String location,
    Handler<HttpClientResponse> onResponse,
    Consumer<Failure> failureCallback) {

    HttpClientRequest request = vertx.createHttpClient()
      .requestAbs(method, location, onResponse);

    registerExceptionHandler(request, failureCallback);
    addOkapiHeaders(request);

    return request;
  }

  private String individualRecordLocation(String id) {
    return String.format("%s/instance-storage/instances/%s", storageAddress, id);
  }

  private Handler<HttpClientResponse> noContentResponseHandler(
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    return response ->
      response.bodyHandler(buffer -> {
        String responseBody = buffer.getString(0, buffer.length());
        int statusCode = response.statusCode();

        if(statusCode == 204) {
          completionCallback.accept(new Success<>(null));
        }
        else {
          failureCallback.accept(new Failure(responseBody, statusCode));
        }
      });
  }
}
